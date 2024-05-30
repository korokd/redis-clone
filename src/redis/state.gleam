import gleam/io

import gleam/int
import gleam/list
import gleam/result
import gleam/string

import argv.{Argv}

import redis/store.{type Store}

pub type Config {
  Master(port: Int, replid: String, repl_offset: Int)
  Slave(port: Int, replicaof: #(String, Int))
}

pub opaque type State {
  State(config: Config, store: Store)
}

pub fn init() -> State {
  let config = parse_arguments()
  let store = store.new()

  State(config, store)
}

fn parse_arguments() -> Config {
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

  case replicaof {
    Ok(master) -> Slave(port, master)

    Error(_) -> {
      let repl_offset = 0
      let replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"

      Master(port, replid, repl_offset)
    }
  }
}

pub fn get_config(state: State) -> Config {
  state.config
}

pub fn get_port(state: State) -> Int {
  case get_config(state) {
    Master(port, _, _) -> port

    Slave(port, _) -> port
  }
}

pub fn get_store(state: State) -> Store {
  state.store
}

pub fn update_store(store: Store, state: State) -> State {
  State(state.config, store)
}
