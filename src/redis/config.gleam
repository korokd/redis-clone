import gleam/io

import gleam/int
import gleam/list
import gleam/regex
import gleam/result
import gleam/string

import argv.{Argv}
import mug.{type Socket}

import redis/command
import redis/master.{type Master}
import redis/resp

pub opaque type MasterInfo {
  MasterInfo(host: String, port: Int)
}

pub type Config {
  Master(port: Int, data: Master)
  Replica(port: Int, master_info: MasterInfo)
}

pub fn init() -> Config {
  let #(port, replicaof) = parse_arguments()

  case replicaof {
    Ok(master) -> {
      let assert Ok(_) = handshake(port, master)

      Replica(port, master)
    }

    Error(_) -> {
      Master(port, master.setup_for_replication())
    }
  }
}

// TODO refactor: actually handle the Errors
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

fn handshake(own_port: Int, master_info: MasterInfo) -> Result(Nil, Nil) {
  let MasterInfo(host, master_port) = master_info

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
  let assert Ok(_) = handshake_psync(socket)
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
    mug.receive(socket, timeout_milliseconds: 1000)
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
    mug.receive(socket, timeout_milliseconds: 1000)
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
    mug.receive(socket, timeout_milliseconds: 1000)
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

fn handshake_psync(socket: Socket) -> Result(Nil, Nil) {
  let replconf_psync =
    command.Psync("?", -1)
    |> command.to_resp_data()
    |> resp.encode()

  use _ <- result.try(
    mug.send(socket, replconf_psync)
    |> result.nil_error(),
  )
  use replconf_psync_response <- result.try(
    mug.receive(socket, timeout_milliseconds: 1000)
    |> result.nil_error(),
  )
  use decoded_replconf_psync_response <- result.try(
    resp.parse(replconf_psync_response)
    |> result.nil_error(),
  )
  let assert resp.Parsed(resp.SimpleString(response_string), excess) =
    decoded_replconf_psync_response

  use response_regex <- result.try(
    regex.compile(
      "FULLRESYNC\\s.+?\\d$",
      with: regex.Options(case_insensitive: True, multi_line: True),
    )
    |> result.nil_error(),
  )
  use _ <- result.try(case
    regex.check(with: response_regex, content: response_string)
  {
    True -> Ok(Nil)

    False -> Error(Nil)
  })

  case excess {
    <<>> ->
      mug.receive(socket, timeout_milliseconds: 1000)
      |> result.nil_error()
      |> result.map(fn(_) { Nil })

    _rdb_file -> Ok(Nil)
  }
}

pub fn get_port(config: Config) -> Int {
  case config {
    Master(port, _) -> port

    Replica(port, _) -> port
  }
}

// TODO refactor: redis/master.get_replication_data and this are too different to be named so similarly
pub fn get_replication_info(config: Config) -> List(String) {
  case config {
    Master(_, master) -> {
      let master.ReplicationData(replid, repl_offset, _) =
        master.get_replication_data(master)

      [
        "role:master",
        "master_replid:" <> replid,
        "master_repl_offset:" <> int.to_string(repl_offset),
      ]
    }

    Replica(_, _) -> ["role:slave"]
  }
}
