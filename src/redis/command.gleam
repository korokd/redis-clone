import gleam/io

import gleam/int
import gleam/option.{type Option, None, Some}
import gleam/string

import redis/resp.{type Parsed, type RespData}

pub type InfoSection {
  Replication
}

pub type ReplConfOption {
  Capabilities(name: String)
  ListeningPort(port: Int)
}

pub type Command {
  Binary(content: String)
  Ping
  Echo(value: RespData)
  Set(key: String, value: RespData, expiry: Option(Int))
  Get(key: String)
  Info(section: InfoSection)
  ReplConf(option: ReplConfOption)
  Psync(replid: String, offset: Int)
}

pub type CommandError {
  UnexpectedArguments(command: String, arguments: List(RespData))
  UnknownCommand(command: String)
}

pub fn to_resp_data(command: Command) -> RespData {
  case command {
    Ping -> resp.Array([resp.BulkString("PING")])

    Set(key, value, expiry) -> {
      let additional_arguments = case expiry {
        Some(expiry) -> [
          resp.BulkString("px"),
          resp.BulkString(int.to_string(expiry)),
        ]

        None -> []
      }

      resp.Array([
        resp.BulkString("SET"),
        resp.BulkString(key),
        value,
        ..additional_arguments
      ])
    }

    ReplConf(Capabilities(name)) ->
      resp.Array([
        resp.BulkString("REPLCONF"),
        resp.BulkString("capa"),
        resp.BulkString(name),
      ])

    ReplConf(ListeningPort(port)) ->
      resp.Array([
        resp.BulkString("REPLCONF"),
        resp.BulkString("listening-port"),
        resp.BulkString(int.to_string(port)),
      ])

    Psync(replid, offset) ->
      resp.Array([
        resp.BulkString("PSYNC"),
        resp.BulkString(replid),
        resp.BulkString(int.to_string(offset)),
      ])

    _ -> panic as "Unsupported command @ `command.to_resp_data`"
  }
}

pub fn from_resp_data(data: Parsed) -> Result(Command, CommandError) {
  let resp.Parsed(resp_data, _) = data

  case resp_data {
    resp.Binary(content) -> Ok(Binary(content))

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
          ]
        ->
          case string.lowercase(px) {
            "px" -> {
              let assert Ok(expiry) = int.parse(expiry)
              Ok(Set(key, value, Some(expiry)))
            }

            _ -> Error(UnexpectedArguments(command, arguments))
          }

        "get", [resp.BulkString(key)] -> Ok(Get(key))

        "info", [resp.BulkString(section)] ->
          case string.lowercase(section) {
            "replication" -> Ok(Info(Replication))

            _ -> Error(UnexpectedArguments(command, arguments))
          }

        "replconf", [resp.BulkString(option), argument] ->
          case option, argument {
            "capa", resp.BulkString(name) -> Ok(ReplConf(Capabilities(name)))

            "listening-port", resp.BulkString(port) ->
              case int.parse(port) {
                Ok(port) -> Ok(ReplConf(ListeningPort(port)))

                Error(_) -> Error(UnexpectedArguments(command, arguments))
              }

            _, _ -> Error(UnexpectedArguments(command, arguments))
          }

        "psync", [resp.BulkString(replid), resp.BulkString(offset)] ->
          case int.parse(offset) {
            Ok(offset) -> Ok(Psync(replid, offset))

            Error(_) -> Error(UnexpectedArguments(command, arguments))
          }

        "ping", _ -> Error(UnexpectedArguments(command, arguments))
        "echo", _ -> Error(UnexpectedArguments(command, arguments))
        "set", _ -> Error(UnexpectedArguments(command, arguments))
        "get", _ -> Error(UnexpectedArguments(command, arguments))
        "info", _ -> Error(UnexpectedArguments(command, arguments))
        "replconf", _ -> Error(UnexpectedArguments(command, arguments))
        "psync", _ -> Error(UnexpectedArguments(command, arguments))

        _, _ -> Error(UnknownCommand(command))
      }
    }

    resp.Null -> {
      panic as "Unexpected Null"
    }
  }
}
