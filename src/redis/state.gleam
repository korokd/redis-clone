import gleam/io

import redis/config.{type Config}
import redis/store.{type Store}

pub opaque type State {
  State(config: Config, store: Store)
}

pub fn init() -> State {
  let config = config.init()
  let store = store.new()

  State(config, store)
}

pub fn get_config(state: State) -> Config {
  state.config
}

pub fn get_store(state: State) -> Store {
  state.store
}

pub fn update_store(store: Store, state: State) -> State {
  State(state.config, store)
}
