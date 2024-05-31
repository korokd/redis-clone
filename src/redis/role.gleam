import gleam/io

import gleam/regex
import gleam/result

import mug.{type Socket}

import redis/command
import redis/master.{type Master}
import redis/resp

pub type Role {
  Master(master: Master)
  Replica
}

pub fn init_master() -> Role {
  Master(master: master.init())
}

pub fn init_replica(
  own_port: Int,
  master_host: String,
  master_port: Int,
) -> Role {
  let assert Ok(_) = handshake(own_port, master_host, master_port)

  Replica
}

// TODO refactor: actually handle the Errors
fn handshake(
  own_port: Int,
  master_host: String,
  master_port: Int,
) -> Result(Nil, Nil) {
  let options =
    mug.new(master_host, port: master_port)
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

pub fn get_info(role: Role) -> List(String) {
  case role {
    Master(master) -> master.get_info(master)

    Replica -> ["role:slave"]
  }
}
