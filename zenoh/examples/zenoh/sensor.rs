//
// Copyright (c) 2017, 2020 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//
use clap::{App, Arg};
use std::convert::{TryFrom, TryInto};
use zenoh::*;
use opentelemetry::trace::TraceError;
use opentelemetry::{
    global,
    sdk::{trace as sdktrace, propagation::TraceContextPropagator},
    trace::{FutureExt, TraceContextExt, Tracer},
    Context,
};
use opentelemetry_jaeger;
use std::collections::HashMap;

#[async_std::main]
async fn main() {
    // initiate logging
    env_logger::init();
    // initate tracer
    let _ = init_global_tracer().unwrap();
    let span = global::tracer("sensor.rs").start("Put data");
    let cx = Context::current_with_span(span);
    let mut injector = HashMap::new();
    global::get_text_map_propagator(|propagator| propagator.inject_context(&cx, &mut injector));

    let (config, path, _value) = parse_args();

    println!("New zenoh...");
    let zenoh = Zenoh::new(config.into()).with_context(cx.clone()).await.unwrap();

    println!("New workspace...");
    let workspace = zenoh.workspace(None).with_context(cx.clone()).await.unwrap();

    println!("Put Data ('{}': '{}')...\n", path, injector["traceparent"]);
    workspace
        .put(&path.try_into().unwrap(), injector["traceparent"].clone().into())
        .with_context(cx.clone())
        .await
        .unwrap();

    // --- Examples of put with other types:

    // - Integer
    // workspace.put(&"/demo/example/Integer".try_into().unwrap(), 3.into())
    //     .await.unwrap();

    // - Float
    // workspace.put(&"/demo/example/Float".try_into().unwrap(), 3.14.into())
    //     .await.unwrap();

    // - Properties (as a Dictionary with str only)
    // workspace.put(
    //         &"/demo/example/Properties".try_into().unwrap(),
    //         Properties::from("p1=v1;p2=v2").into()
    //     ).await.unwrap();

    // - Json (str format)
    // workspace.put(
    //         &"/demo/example/Json".try_into().unwrap(),
    //         Value::Json(r#"{"kind"="memory"}"#.to_string()),
    //     ).await.unwrap();

    // - Raw ('application/octet-stream' encoding by default)
    // workspace.put(
    //         &"/demo/example/Raw".try_into().unwrap(),
    //         vec![0x48u8, 0x69, 0x33].into(),
    //     ).await.unwrap();

    // - Custom
    // workspace.put(
    //         &"/demo/example/Custom".try_into().unwrap(),
    //         Value::Custom {
    //             encoding_descr: "my_encoding".to_string(),
    //             data: vec![0x48u8, 0x69, 0x33].into(),
    //     }).await.unwrap();

    zenoh.close().with_context(cx).await.unwrap();
    opentelemetry::global::force_flush_tracer_provider();
    opentelemetry::global::shutdown_tracer_provider();
}

fn init_global_tracer() -> Result<sdktrace::Tracer, TraceError>{
    global::set_text_map_propagator(TraceContextPropagator::new());
    // let tags = [
    //     resource::SERVICE_VERSION.string(version.to_owned()),
    //     resource::SERVICE_INSTANCE_ID.string(instance_id.to_owned()),
    //     resource::PROCESS_EXECUTABLE_PATH.string(std::env::current_exe().unwrap().display().to_string()),
    //     resource::PROCESS_PID.string(std::process::id().to_string()),
    //     KeyValue::new("process.executable.profile", PROFILE),
    // ];

    opentelemetry_jaeger::new_pipeline()
        .with_service_name("sensor")
        //.with_tags(tags.iter().map(ToOwned::to_owned))
        .install_batch(opentelemetry::runtime::AsyncStd)
}

fn parse_args() -> (Properties, String, String) {
    let args = App::new("zenoh put example")
        .arg(
            Arg::from_usage("-m, --mode=[MODE] 'The zenoh session mode (peer by default).")
                .possible_values(&["peer", "client"]),
        )
        .arg(Arg::from_usage(
            "-e, --peer=[LOCATOR]...  'Peer locators used to initiate the zenoh session.'",
        ))
        .arg(Arg::from_usage(
            "-l, --listener=[LOCATOR]...   'Locators to listen on.'",
        ))
        .arg(Arg::from_usage(
            "-c, --config=[FILE]      'A configuration file.'",
        ))
        .arg(
            Arg::from_usage("-p, --path=[PATH]        'The name of the resource to put.'")
                .default_value("/demo/example/zenoh-rs-put"),
        )
        .arg(
            Arg::from_usage("-v, --value=[VALUE]      'The value of the resource to put.'")
                .default_value("Put from Rust!"),
        )
        .arg(Arg::from_usage(
            "--no-multicast-scouting 'Disable the multicast-based scouting mechanism.'",
        ))
        .get_matches();

    let mut config = if let Some(conf_file) = args.value_of("config") {
        Properties::try_from(std::path::Path::new(conf_file)).unwrap()
    } else {
        Properties::default()
    };
    for key in ["mode", "peer", "listener"].iter() {
        if let Some(value) = args.values_of(key) {
            config.insert(key.to_string(), value.collect::<Vec<&str>>().join(","));
        }
    }
    if args.is_present("no-multicast-scouting") {
        config.insert("multicast_scouting".to_string(), "false".to_string());
    }

    let path = args.value_of("path").unwrap().to_string();
    let value = args.value_of("value").unwrap().to_string();

    (config, path, value)
}
