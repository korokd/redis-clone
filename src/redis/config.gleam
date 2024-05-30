import gleam/int
import gleam/list
import gleam/result
import gleam/string

import argv.{Argv}
import mug

import redis/resp

type ReplicaOf =
  #(String, Int)

pub opaque type Config {
  Master(port: Int, replid: String, repl_offset: Int)
  Slave(port: Int, replicaof: #(String, Int))
}

pub fn init() -> Config {
  let #(port, replicaof) = parse_arguments()

  case replicaof {
    Ok(master) -> {
      Slave(port, master)
    }

    Error(_) -> {
      let repl_offset = 0
      let replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"

      Master(port, replid, repl_offset)
    }
  }
}

fn parse_arguments() -> #(Int, Result(ReplicaOf, Nil)) {
  let Argv(_, _, args) = argv.load()

  let port =
    list.window_by_2(args)
    |> list.find_map(fn(window) {
      case window.0 == "--port" {
        False -> Error(Nil)
        True ->
          case int.parse(window.1) {
            Ok(port) -> Ok(port)
            Error(_) -> panic as { "Cannot parse port: " <> window.1 }
          }
      }
    })
    |> result.unwrap(6379)

  let replicaof =
    list.window_by_2(args)
    |> list.find_map(fn(window) {
      case window.0 == "--replicaof" {
        False -> Error(Nil)
        True -> {
          let assert [host, port] = string.split(window.1, on: " ")
          case int.parse(port) {
            Ok(port) -> Ok(#(host, port))
            Error(_) -> panic as { "Cannot parse master's port: " <> window.1 }
          }
        }
      }
    })

  #(port, replicaof)
}

pub fn get_port(config: Config) -> Int {
  case config {
    Master(port, _, _) -> port

    Slave(port, _) -> port
  }
}

pub fn get_replication_info(config: Config) {
  case config {
    Master(_, replid, repl_offset) -> [
      "role:master",
      "master_replid:" <> replid,
      "master_repl_offset:" <> int.to_string(repl_offset),
    ]

    Slave(_, _) -> ["role:slave"]
  }
}
