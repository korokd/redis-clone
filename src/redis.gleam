import gleam/io

import gleam/bit_array
import gleam/bytes_builder
import gleam/dict
import gleam/erlang/process
import gleam/list
import gleam/option.{None}
import gleam/otp/actor
import gleam/string
import glisten.{Packet, User}

type Command {
  Ping
  Echo(value: String)
  Set(key: String, value: String)
  Get(key: String)
}

type CommandError {
  UnexpectedArguments(command: String, arguments: List(String))
  UnknownCommand(command: String)
}

type State =
  dict.Dict(String, String)

pub fn main() {
  let state: State = dict.new()

  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(state, None) }, loop)
    |> glisten.serve(6379)

  process.sleep_forever()
}

fn loop(
  msg: glisten.Message(a),
  state: State,
  conn: glisten.Connection(a),
) -> actor.Next(glisten.Message(a), State) {
  case msg {
    Packet(data) -> handle_message(data, state, conn)
    User(_) -> actor.continue(state)
  }
}

fn handle_message(
  msg: BitArray,
  state: State,
  conn: glisten.Connection(a),
) -> actor.Next(glisten.Message(a), State) {
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

  io.debug(command)
  io.debug(arguments)

  #(command, arguments)
}

fn parse_command(
  command: String,
  arguments: List(String),
) -> Result(Command, CommandError) {
  case string.lowercase(command), arguments {
    "ping", [] -> Ok(Ping)
    "echo", [value] -> Ok(Echo(value))
    "set", [key, value] -> Ok(Set(key, value))
    "get", [key] -> Ok(Get(key))

    "ping", _ -> Error(UnexpectedArguments(command, arguments))
    "echo", _ -> Error(UnexpectedArguments(command, arguments))
    "set", _ -> Error(UnexpectedArguments(command, arguments))
    "get", _ -> Error(UnexpectedArguments(command, arguments))

    _, _ -> Error(UnknownCommand(command))
  }
}

fn handle_command(
  command: Command,
  state: State,
  conn: glisten.Connection(a),
) -> actor.Next(glisten.Message(a), State) {
  let _respond = case command {
    Ping -> {
      let pong = "+PONG\r\n"
      let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(pong))

      actor.continue(state)
    }

    Echo(value) -> {
      let response = "+" <> value <> "\r\n"
      let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(response))

      actor.continue(state)
    }

    Set(key, value) -> {
      let ok = "+OK\r\n"
      let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(ok))

      dict.insert(into: state, for: key, insert: value)
      |> actor.continue()
    }

    Get(key) -> {
      let assert Ok(value) = dict.get(state, key)
      let response = "+" <> value <> "\r\n"
      let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(response))

      actor.continue(state)
    }
  }
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
