import gleam/io

import gleam/int
import gleam/list
import gleam/result
import gleam/string

import argv.{Argv}

import redis/replica.{type Replica}
import redis/replication.{type Replication}
import redis/store.{type Store}

pub type Config {
  Master(port: Int, master: Replication)
  Replica(port: Int, replica: Replica)
}

pub fn init(store: Store) -> Config {
  let #(own_port, replicaof) = parse_arguments()

  case replicaof {
    Ok(#(master_host, master_port)) -> {
      init_replica(store, own_port, master_host, master_port)
    }

    Error(_) -> {
      init_master(own_port)
    }
  }
}

fn parse_arguments() -> #(Int, Result(#(String, Int), Nil)) {
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

fn init_master(port: Int) -> Config {
  Master(port: port, master: replication.init())
}

fn init_replica(
  store: Store,
  own_port: Int,
  master_host: String,
  master_port: Int,
) -> Config {
  Replica(
    port: own_port,
    replica: replica.init(store, own_port, master_host, master_port),
  )
}

pub fn get_info(config: Config) -> List(String) {
  case config {
    Master(_port, master) -> replication.get_info(master)

    Replica(_port, _replica) -> replica.get_info()
  }
}

pub fn get_port(config: Config) -> Int {
  config.port
}
