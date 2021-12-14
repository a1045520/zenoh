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
use futures::prelude::*;
use std::convert::{TryFrom, TryInto};
use zenoh::*;
use opentelemetry::trace::TraceError;
use opentelemetry::{
    global,
    sdk::{trace as sdktrace, propagation::TraceContextPropagator},
    trace::{FutureExt, TraceContextExt, Tracer},
    Context,
    KeyValue,
};
use opentelemetry_jaeger;
use std::collections::HashMap;

#[async_std::main]
async fn main() {
    // initiate logging
    env_logger::init();
    // initiate tracer
    let _ = init_global_tracer().unwrap();
    let span = global::tracer("t_get.rs").start("Root");
    let cx = Context::current_with_span(span);
    let mut injector = HashMap::new();
    global::get_text_map_propagator(|propagator| propagator.inject_context(&cx, &mut injector));

    // for (k, v) in &injector{
    //     println!("key is {}, value is {}", k, v);
    // }
    let (config, selector) = parse_args();

    println!("New zenoh...");
    let zenoh = Zenoh::new(config.into()).with_context(cx.clone()).await.unwrap();

    println!("New workspace...");
    let workspace = zenoh.workspace(None).with_context(cx.clone()).await.unwrap();
    // use if let && currently sent traceparent only.
    if injector.contains_key("traceparent") {
        println!("Put Span Data ('{}')...\n", injector["traceparent"]);
        workspace
            .put(&"/demo/example/eval".try_into().unwrap(), injector["traceparent"].clone().into())
            .with_context(cx.clone())
            .await
            .unwrap();
    }

    println!("Get Data from {}'...\n", selector);
    let mut data_stream = workspace.get(&selector.try_into().unwrap()).with_context(cx.clone()).await.unwrap();
    while let Some(data) = data_stream.next().with_context(cx.clone()).await{
        println!(
            "  {} : {:?} (encoding: {} , timestamp: {})",
            data.path,
            data.value,
            data.value.encoding_descr(),
            data.timestamp
        );
        cx.span().add_event(
            "Get the return data".to_string(),
            vec![KeyValue::new("data", format!("{:?}", data.value))],
        );
    }
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
        .with_service_name("t_get")
        //.with_tags(tags.iter().map(ToOwned::to_owned))
        .install_batch(opentelemetry::runtime::AsyncStd)
}

fn parse_args() -> (Properties, String) {
    let args = App::new("zenoh get example")
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
            Arg::from_usage("-s, --selector=[SELECTOR] 'The selection of resources to get'")
                .default_value("/demo/example/**"),
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

    let selector = args.value_of("selector").unwrap().to_string();

    (config, selector)
}
