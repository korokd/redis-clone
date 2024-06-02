import gleam/io

import gleam/erlang/process.{type Selector, type Subject}
import gleam/function
import gleam/otp/actor.{type Next, type StartError}
import gleam/regex
import gleam/result

import mug.{type Socket, type TcpMessage}

import redis/command
import redis/resp

pub type Replica =
  Subject(TcpMessage)

pub fn init(own_port: Int, master_host: String, master_port: Int) -> Replica {
  let assert Ok(replica) = init_actor(own_port, master_host, master_port)

  replica
}

fn init_actor(
  own_port: Int,
  master_host: String,
  master_port: Int,
) -> Result(Replica, StartError) {
  let msg_selector =
    process.new_selector()
    |> mug.selecting_tcp_messages(function.identity)

  actor.start_spec(
    actor.Spec(
      init: fn() {
        actor_init(own_port, master_host, master_port, msg_selector)
      },
      init_timeout: 5000,
      loop: fn(msg, state) { actor_loop(msg, state) },
    ),
  )
}

fn actor_init(
  own_port: Int,
  master_host: String,
  master_port: Int,
  msg_selector: Selector(a),
) {
  let assert Ok(socket) = handshake(own_port, master_host, master_port)

  mug.receive_next_packet_as_message(socket)

  actor.Ready(state: Nil, selector: msg_selector)
}

fn actor_loop(msg, _state) -> Next(TcpMessage, Nil) {
  case msg {
    mug.Packet(socket, _msg) -> {
      mug.receive_next_packet_as_message(socket)

      actor.continue(Nil)
    }

    mug.SocketClosed(_socket) -> actor.Stop(process.Normal)

    mug.TcpError(socket, _error) -> {
      mug.receive_next_packet_as_message(socket)

      actor.continue(Nil)
    }
  }
}

// TODO refactor: actually handle the Errors
fn handshake(
  own_port: Int,
  master_host: String,
  master_port: Int,
) -> Result(Socket, Nil) {
  let options =
    mug.new(master_host, port: master_port)
    |> mug.timeout(milliseconds: 500)
  let assert Ok(socket) = mug.connect(options)

  let assert Ok(_) = handshake_ping(socket)
  let assert Ok(_) = handshake_port(socket, own_port)
  let assert Ok(_) = handshake_capa(socket)
  let assert Ok(_) = handshake_psync(socket)

  Ok(socket)
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

pub fn get_info() -> List(String) {
  ["role:slave"]
}
