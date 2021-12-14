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
use futures::select;
use std::convert::{TryFrom, TryInto};
use zenoh::*;
use opentelemetry::trace::TraceError;
use opentelemetry::{
    global,
    sdk::{trace as sdktrace, propagation::TraceContextPropagator},
    trace::{Tracer, TraceContextExt},
    Context,
};
use opentelemetry_semantic_conventions::{resource, trace};
use opentelemetry_jaeger;
use std::collections::HashMap;
use std::{time, thread};

#[async_std::main]
async fn main() {
    // initiate logging
    env_logger::init();
    // initate tracer
    let _ = init_global_tracer().unwrap();

    let (config, selector) = parse_args();

    println!("New zenoh...");
    let zenoh = Zenoh::new(config.into()).await.unwrap();

    println!("New workspace...");
    let workspace = zenoh.workspace(None).await.unwrap();

    println!("Subscribe to {}'...\n", selector);
    let mut change_stream = workspace
        .subscribe(&selector.try_into().unwrap())
        .await
        .unwrap();

    let mut stdin = async_std::io::stdin();
    let mut input = [0u8];
    loop {
        select!(
            change = change_stream.next().fuse() => {
                let change = change.unwrap();
                // Read the trace format
                let mut req_header = HashMap::new();
                let value = match change.value.unwrap(){
                    Value::StringUtf8(value) => value,
                    _ => String::from("other data type"),
                };
                req_header.insert("traceparent".to_string(), value.clone());

                let parent_cx = global::get_text_map_propagator(|propagator| propagator.extract(&req_header));
                drop(req_header);
                let tracer = global::tracer("z_eval.rs");
                let span = tracer
                    .span_builder("Get and process data")
                    .with_attributes(vec![
                        trace::MESSAGING_SYSTEM.string("zenoh"),
                        trace::MESSAGING_OPERATION.string("receive"),
                    ])
                    .with_parent_context(parent_cx)
                    .start(&tracer);
                let cx = Context::current_with_span(span);

                cx.span().add_event("Start process data".into(), vec![]);
                // Sleep for simulation some calculation
                thread::sleep(time::Duration::from_millis(50));
                cx.span().add_event("Finish process".into(), vec![]);

                println!(
                    ">> [Subscription listener] received {:?} for {} : {:?} with timestamp {}",
                    change.kind,
                    change.path,
                    value,
                    change.timestamp
                )
            }

            _ = stdin.read_exact(&mut input).fuse() => {
                if input[0] == b'q' {break}
            }
        );
    }

    change_stream.close().await.unwrap();
    zenoh.close().await.unwrap();
    opentelemetry::global::force_flush_tracer_provider();
    opentelemetry::global::shutdown_tracer_provider();
}

fn init_global_tracer() -> Result<sdktrace::Tracer, TraceError>{
    global::set_text_map_propagator(TraceContextPropagator::new());
    let tags = [
        resource::SERVICE_VERSION.string(env!("CARGO_PKG_VERSION").to_owned()),
        resource::PROCESS_EXECUTABLE_PATH.string(std::env::current_exe().unwrap().display().to_string()),
        resource::PROCESS_PID.string(std::process::id().to_string())
    ];

    opentelemetry_jaeger::new_pipeline()
        .with_service_name("z_sub")
        .with_tags(tags.iter().map(ToOwned::to_owned))
        .install_batch(opentelemetry::runtime::AsyncStd)
}

fn parse_args() -> (Properties, String) {
    let args = App::new("zenoh subscriber example")
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
            Arg::from_usage("-s, --selector=[selector] 'The selection of resources to subscribe'")
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
