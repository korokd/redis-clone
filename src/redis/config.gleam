import gleam/int
import gleam/list
import gleam/result
import gleam/string

import argv.{Argv}
import mug.{type Socket}

import redis/command
import redis/resp

type MasterInfo {
  MasterInfo(host: String, port: Int)
}

pub opaque type Config {
  Master(port: Int, replid: String, repl_offset: Int)
  Slave(port: Int, master_info: MasterInfo)
}

pub fn init() -> Config {
  let #(port, replicaof) = parse_arguments()

  case replicaof {
    Ok(master) -> {
      let assert Ok(_) = handshake(port, master)

      Slave(port, master)
    }

    Error(_) -> {
      let repl_offset = 0
      let replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"

      Master(port, replid, repl_offset)
    }
  }
}

fn parse_arguments() -> #(Int, Result(MasterInfo, Nil)) {
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
            Ok(port) -> Ok(MasterInfo(host, port))
            Error(_) -> panic as { "Cannot parse master's port: " <> window.1 }
          }
        }
      }
    })

  #(port, replicaof)
}

fn handshake(own_port: Int, master: MasterInfo) -> Result(Nil, Nil) {
  let MasterInfo(host, master_port) = master

  let options =
    mug.new(host, port: master_port)
    |> mug.timeout(milliseconds: 500)

  use socket <- result.try(
    mug.connect(options)
    |> result.nil_error(),
  )

  let assert Ok(_) = handshake_ping(socket)
  let assert Ok(_) = handshake_port(socket, own_port)
  let assert Ok(_) = handshake_capa(socket)
}

fn handshake_ping(socket: Socket) -> Result(Nil, Nil) {
  let ping =
    command.Ping
    |> command.to_resp_data()
    |> resp.encode()

  use _ <- result.try(
    mug.send(socket, ping)
    |> result.nil_error(),
  )
  use ping_response <- result.try(
    mug.receive(socket, 1000)
    |> result.nil_error(),
  )
  use decoded_ping_response <- result.try(
    resp.parse(ping_response)
    |> result.nil_error(),
  )
  let assert resp.Parsed(resp.SimpleString("PONG"), <<>>) =
    decoded_ping_response

  Ok(Nil)
}

fn handshake_port(socket: Socket, own_port: Int) -> Result(Nil, Nil) {
  let replconf_port =
    command.ReplConf(command.ListeningPort(own_port))
    |> command.to_resp_data()
    |> resp.encode()

  use _ <- result.try(
    mug.send(socket, replconf_port)
    |> result.nil_error(),
  )
  use replconf_port_response <- result.try(
    mug.receive(socket, 1000)
    |> result.nil_error(),
  )
  use decoded_replconf_port_response <- result.try(
    resp.parse(replconf_port_response)
    |> result.nil_error(),
  )
  let assert resp.Parsed(resp.SimpleString("OK"), <<>>) =
    decoded_replconf_port_response

  Ok(Nil)
}

fn handshake_capa(socket: Socket) -> Result(Nil, Nil) {
  let replconf_capa =
    command.ReplConf(command.Capabilities("psync2"))
    |> command.to_resp_data()
    |> resp.encode()

  use _ <- result.try(
    mug.send(socket, replconf_capa)
    |> result.nil_error(),
  )
  use replconf_capa_response <- result.try(
    mug.receive(socket, 1000)
    |> result.nil_error(),
  )
  use decoded_replconf_capa_response <- result.try(
    resp.parse(replconf_capa_response)
    |> result.nil_error(),
  )
  let assert resp.Parsed(resp.SimpleString("OK"), <<>>) =
    decoded_replconf_capa_response

  Ok(Nil)
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
