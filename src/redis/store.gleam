import gleam/io

import gleam/bytes_builder
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor.{type Next}
import gleam/result

import birl

import redis/resp

pub opaque type StoreError {
  Expired
  NotFound
}

type Key =
  String

type ValueRaw =
  resp.RespData

type Expiry =
  Option(Int)

pub opaque type Metadata {
  Metadata(created_at: Int, updated_at: Int, expiry: Option(Int))
}

type Value =
  #(ValueRaw, Metadata)

pub type Store =
  Subject(StoreMessage)

pub type StoreData {
  StoreData(store: Dict(Key, Value))
}

pub opaque type StoreMessage {
  GetStoreData(Subject(StoreData))
  Set(key: Key, value: ValueRaw, expiry: Expiry)
}

pub fn init() -> Store {
  let assert Ok(store) =
    actor.start(StoreData(store: dict.new()), handle_storage)

  store
}

fn handle_storage(
  msg: StoreMessage,
  state: StoreData,
) -> Next(StoreMessage, StoreData) {
  case msg {
    GetStoreData(subject) -> {
      process.send(subject, state)

      actor.continue(state)
    }

    Set(key, value, expiry) -> {
      let updated_at =
        birl.now()
        |> birl.to_unix_milli()

      let created_at = case dict.get(state.store, key) {
        Ok(#(_, Metadata(created_at, _, _))) -> created_at
        Error(_) -> updated_at
      }

      let metadata = Metadata(created_at, updated_at, expiry)

      let store = dict.insert(state.store, key, #(value, metadata))
      let state = StoreData(store: store)

      actor.continue(state)
    }
  }
}

pub fn upsert(store: Store, key: Key, value: ValueRaw, expiry: Expiry) -> Nil {
  process.send(store, Set(key, value, expiry))
}

pub fn get(store: Store, key: Key) -> Result(ValueRaw, StoreError) {
  let StoreData(store) = process.call(store, GetStoreData, 10)
  case dict.get(store, key) {
    Ok(#(value, Metadata(_, updated_at, expiry))) -> {
      let now =
        birl.now()
        |> birl.to_unix_milli()

      case expiry {
        Some(expiry) ->
          case now > updated_at + expiry {
            True -> Error(Expired)
            False -> Ok(value)
          }

        None -> Ok(value)
      }
    }

    Error(_) -> Error(NotFound)
  }
}
