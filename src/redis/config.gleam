import gleam/io

import gleam/int
import gleam/list
import gleam/result
import gleam/string

import argv.{Argv}

import redis/role.{type Role}

pub opaque type Config {
  Config(port: Int, role: Role)
}

pub fn init() -> Config {
  let #(own_port, replicaof) = parse_arguments()

  case replicaof {
    Ok(#(master_host, master_port)) -> {
      Config(
        port: own_port,
        role: role.init_replica(own_port, master_host, master_port),
      )
    }

    Error(_) -> {
      Config(port: own_port, role: role.init_master())
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

pub fn get_port(config: Config) -> Int {
  config.port
}

pub fn get_role(config: Config) -> Role {
  config.role
}
