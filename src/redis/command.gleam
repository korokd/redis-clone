import gleam/io

import gleam/int
import gleam/option.{type Option, None, Some}
import gleam/string

import redis/resp

pub type InfoSection {
  Replication
}

pub type Command {
  Ping
  Echo(value: resp.RespData)
  Set(key: String, value: resp.RespData, expiry: Option(Int))
  Get(key: String)
  Info(InfoSection)
}

pub type CommandError {
  UnexpectedArguments(command: String, arguments: List(resp.RespData))
  UnknownCommand(command: String)
}

pub fn from_resp_data(data: resp.Parsed) -> Result(Command, CommandError) {
  let resp.Parsed(resp_data, _) = data

  case resp_data {
    resp.SimpleString(command) ->
      case command {
        "ping" -> Ok(Ping)

        _ -> Error(UnknownCommand(command))
      }

    resp.BulkString(command) ->
      case command {
        "ping" -> Ok(Ping)

        _ -> Error(UnknownCommand(command))
      }

    resp.Array(elements) -> {
      let assert [resp.BulkString(command), ..arguments] = elements

      case string.lowercase(command), arguments {
        "ping", [] -> Ok(Ping)

        "echo", [value] -> Ok(Echo(value))

        "set", [resp.BulkString(key), value] -> Ok(Set(key, value, None))
        "set",
          [
            resp.BulkString(key),
            value,
            resp.BulkString(px),
            resp.BulkString(expiry),
          ] ->
          case string.lowercase(px) {
            "px" -> {
              let assert Ok(expiry) = int.parse(expiry)
              Ok(Set(key, value, Some(expiry)))
            }

            _ -> Error(UnexpectedArguments(command, arguments))
          }

        "get", [resp.BulkString(key)] -> Ok(Get(key))

        "info", [resp.BulkString(replication)] ->
          case string.lowercase(replication) {
            "replication" -> Ok(Info(Replication))

            _ -> Error(UnexpectedArguments(command, arguments))
          }

        "ping", _ -> Error(UnexpectedArguments(command, arguments))
        "echo", _ -> Error(UnexpectedArguments(command, arguments))
        "set", _ -> Error(UnexpectedArguments(command, arguments))
        "get", _ -> Error(UnexpectedArguments(command, arguments))

        _, _ -> Error(UnknownCommand(command))
      }
    }

    resp.Null -> {
      panic as "Unexpected Null"
    }
  }
}
