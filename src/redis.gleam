import gleam/io

import gleam/bit_array
import gleam/bytes_builder
import gleam/erlang/process
import gleam/list
import gleam/option.{None}
import gleam/otp/actor
import gleam/string
import glisten.{Packet, User}

pub fn main() {
  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, loop)
    |> glisten.serve(6379)

  process.sleep_forever()
}

fn loop(msg: glisten.Message(a), state: state, conn: glisten.Connection(a)) -> actor.Next(glisten.Message(a), state) {
  case msg {
    Packet(data) -> handle_message(data, state, conn)
    User(_) -> actor.continue(state)
  }
}

fn handle_message(msg: BitArray, state: state, conn: glisten.Connection(a)) -> actor.Next(glisten.Message(a), state) {
  let assert Ok(msg) = bit_array.to_string(msg)
  let parts = string.split(msg, on: "\r\n")
  let assert [_how_many, ..tail] = parts
  let assert [command, ..arguments] = list.index_fold(over: tail, from: [], with: fn (acc, s, i) {
    let is_odd = i % 2 != 0
    case is_odd {
      True -> [s, ..acc]
      False -> acc
    }
  }) |> list.reverse()

  case string.lowercase(command), arguments {
    "ping", _ -> handle_ping(state, conn)
    "echo", [value] -> handle_echo(state, conn, value)
    "echo", _ -> unexpected_arguments(command, arguments)
    _, _ -> unhandled(command)
  }

  actor.continue(state)
}

fn handle_ping(state: state, conn: glisten.Connection(a)) -> actor.Next(glisten.Message(a), state) {
  let pong = "+PONG\r\n"
  let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(pong))
  actor.continue(state)
}

fn handle_echo(state: state, conn: glisten.Connection(a), value: String) -> actor.Next(glisten.Message(a), state) {
  let pong = "+" <> value <> "\r\n"
  let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(pong))
  actor.continue(state)
}

fn unexpected_arguments(command: String, arguments: List(String)) {
  panic as { "Unexpected arguments for command " <> command <> ": " <> string.inspect(arguments) }
}

fn unhandled(command: String) {
  panic as { "Unknown command: " <> command }
}
