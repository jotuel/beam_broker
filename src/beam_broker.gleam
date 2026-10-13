import beam_broker/supervisor
import gleam/erlang/process
import logging
import wisp

pub fn main() -> Nil {
  logging.configure()
  wisp.configure_logger()
  logging.log(logging.Info, "starting beam_broker")
  case supervisor.start_orchestrator() {
    Ok(_) -> {
      process.sleep_forever()
    }
    Error(_) -> logging.log(logging.Error, "failed to start orchestrator")
  }
}
