import gleam/io

import gleam/bit_array
import gleam/bytes_builder
import gleam/erlang/process
import gleam/list
import gleam/option.{None}
import gleam/otp/actor
import gleam/string
import glisten.{Packet, User}

type Command {
  Ping
  Echo(value: String)
}

type CommandError {
  UnexpectedArguments(command: String, arguments: List(String))
  UnknownCommand(command: String)
}

pub fn main() {
  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, loop)
    |> glisten.serve(6379)

  process.sleep_forever()
}

fn loop(
  msg: glisten.Message(a),
  state: state,
  conn: glisten.Connection(a),
) -> actor.Next(glisten.Message(a), state) {
  case msg {
    Packet(data) -> handle_message(data, state, conn)
    User(_) -> actor.continue(state)
  }
}

fn handle_message(
  msg: BitArray,
  state: state,
  conn: glisten.Connection(a),
) -> actor.Next(glisten.Message(a), state) {
  let #(command, arguments) = parse_message(msg)

  case parse_command(command, arguments) {
    Ok(command) -> handle_command(command, state, conn)
    Error(error) -> handle_command_error(error)
  }
}

fn parse_message(msg: BitArray) -> #(String, List(String)) {
  let assert Ok(msg) = bit_array.to_string(msg)
  let parts = string.split(msg, on: "\r\n")
  let assert [_how_many, ..tail] = parts
  let assert [command, ..arguments] =
    list.index_fold(over: tail, from: [], with: fn(acc, s, i) {
      let is_odd = i % 2 != 0
      case is_odd {
        True -> [s, ..acc]
        False -> acc
      }
    })
    |> list.reverse()

  #(command, arguments)
}

fn parse_command(
  command: String,
  arguments: List(String),
) -> Result(Command, CommandError) {
  case string.lowercase(command), arguments {
    "ping", _ -> Ok(Ping)
    "echo", [value] -> Ok(Echo(value))
    "echo", _ -> Error(UnexpectedArguments(command, arguments))
    _, _ -> Error(UnknownCommand(command))
  }
}

fn handle_command(
  command: Command,
  state: state,
  conn: glisten.Connection(a),
) -> actor.Next(glisten.Message(a), state) {
  let _respond = case command {
    Ping -> {
      let pong = "+PONG\r\n"
      let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(pong))
    }

    Echo(value) -> {
      let pong = "+" <> value <> "\r\n"
      let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(pong))
    }
  }

  actor.continue(state)
}

fn handle_command_error(error: CommandError) {
  case error {
    UnexpectedArguments(command, arguments) -> {
      panic as {
        "Unexpected arguments for command "
        <> command
        <> ": "
        <> string.inspect(arguments)
      }
    }

    UnknownCommand(command) -> {
      panic as { "Unknown command: " <> command }
    }
  }
}
