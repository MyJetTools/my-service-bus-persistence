use crate::app::AppContext;

use my_service_bus_shared::protobuf_models::MessageProtobufModel;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NewMessagesProtobufContract {
    #[prost(string, tag = "1")]
    pub topic_id: ::prost::alloc::string::String,

    #[prost(repeated, message, tag = "2")]
    pub messages: Vec<MessageProtobufModel>,
}

impl NewMessagesProtobufContract {
    pub fn parse(protobuf: &[u8]) -> Self {
        prost::Message::decode(protobuf).unwrap()
    }
}

pub fn check_flags(app: &AppContext) -> Result<(), tonic::Status> {
    if !app.app_states.is_initialized() {
        return Err(tonic::Status::cancelled(
            "Application is not initialized yet",
        ));
    }

    if app.app_states.is_shutting_down() {
        return Err(tonic::Status::cancelled("Shutting down"));
    }

    Ok(())
}
