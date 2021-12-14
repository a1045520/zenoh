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
use std::convert::TryFrom;
use zenoh::*;
use opentelemetry::{
    global,
    sdk::{trace as sdktrace, propagation::TraceContextPropagator},
    trace::{FutureExt, TraceContextExt, Tracer},
    Context,
};
// use opentelemetry_semantic_conventions::{resource, trace};
use opentelemetry::trace::TraceError;
use opentelemetry_jaeger;
use std::collections::HashMap;
use std::{time, thread};

#[async_std::main]
async fn main() {
    // initiate logging
    env_logger::init();
    // initiate tracer
    let _ = init_global_tracer().unwrap();

    let (config, path) = parse_args();

    // NOTE: in this example we choosed to register the eval for a single Path,
    // and to send replies with this same Path.
    // But we could also register an eval for a PathExpr. In this case,
    // the eval implementation should choose the Path(s) for reply(ies) that
    // are coherent with the Selector received in GetRequest.
    let path = &Path::try_from(path).unwrap();

    println!("New zenoh...");
    let zenoh = Zenoh::new(config.into()).await.unwrap();

    println!("New workspace...");
    let workspace = zenoh.workspace(None).await.unwrap();

    let mut get_stream = workspace.register_eval(&path.into()).await.unwrap();

    println!("Subscribe to {}'...\n", path);
    let mut change_stream = workspace
    .subscribe(&path.into())
    .await
    .unwrap();

    let change = change_stream.next().await.unwrap();
    println!(
        ">> [Subscription listener] received {:?} for {} : {:?} with timestamp {}",
        change.kind,
        change.path,
        change.value,
        change.timestamp
    );
    // read the trace format
    let mut req_header = HashMap::new();
    if let Value::StringUtf8(value) =  change.value.unwrap(){
        req_header.insert("traceparent".to_string(), value);
    };

    println!("Register eval for {}'...\n", path);
    
    while let Some(get_request) = get_stream.next().await{
        let parent_cx = global::get_text_map_propagator(|propagator| propagator.extract(&req_header));
        let span = global::tracer("t_eval.rs").start_with_context("Request time", parent_cx);
        let cx = Context::current_with_span(span);
        thread::sleep(time::Duration::from_secs(1));
        println!(
            ">> [Eval listener] received get with selector: {}",
            get_request.selector
        );

        // The returned Value is a StringValue with a 'name' part which is set in 3 possible ways,
        // depending the properties specified in the selector. For example, with the
        // following selectors:
        // - "/zenoh/example/eval" : no properties are set, a default value is used for the name
        // - "/zenoh/example/eval?(name=Bob)" : "Bob" is used for the name
        // - "/zenoh/example/eval?(name=/zenoh/example/name)" : the Eval function does a GET
        //      on "/zenoh/example/name" an uses the 1st result for the name
        let mut name = get_request
            .selector
            .properties
            .get("name")
            .cloned()
            .unwrap_or_else(|| "Rust!".to_string());
        if name.starts_with('/') {
            println!("   >> Get name to use from path: {}", name);
            if let Ok(selector) = Selector::try_from(name.as_str()) {
                match workspace.get(&selector).await.unwrap().next().with_context(cx.clone()).await {
                    Some(Data {
                        path: _,
                        value: Value::StringUtf8(s),
                        timestamp: _,
                    }) => name = s,
                    Some(_) => println!("Failed to get name from '{}' : not a UTF-8 String", name),
                    None => println!("Failed to get name from '{}' : not found", name),
                }
            } else {
                println!(
                    "Failed to get value from '{}' : this is not a valid Selector",
                    name
                );
            }
        }
        let s = format!("Eval from {}", name);
        println!(r#"   >> Returning string: "{}""#, s);
        get_request.reply_async(path.clone(), s.into()).with_context(cx.clone()).await;
    }
    change_stream.close().await.unwrap();
    get_stream.close().await.unwrap();
    zenoh.close().await.unwrap();

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
        .with_service_name("t_eval")
        //.with_tags(tags.iter().map(ToOwned::to_owned))
        .install_batch(opentelemetry::runtime::AsyncStd)
}

fn parse_args() -> (Properties, String) {
    let args = App::new("zenoh eval example")
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
            Arg::from_usage("-p, --path=[PATH] 'The path the eval will respond for'")
                .default_value("/demo/example/eval"),
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

    (config, path)
}
