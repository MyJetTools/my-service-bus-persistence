use crate::app::AppContext;

use my_service_bus_shared::{bcl::BclToUnixMicroseconds, MessageProtobufModel};

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

impl BclToUnixMicroseconds for crate::persistence_grpc::DateTime {
    fn to_unix_microseconds(&self) -> Result<i64, String> {
        my_service_bus_shared::bcl::bcl_date_time_utils::to_unix_microseconds(
            self.value, self.scale,
        )
    }

    fn to_date_time(
        &self,
    ) -> Result<my_service_bus_shared::date_time::DateTimeAsMicroseconds, String> {
        my_service_bus_shared::bcl::bcl_date_time_utils::to_date_time(self)
    }

    fn to_rfc3339(&self) -> String {
        my_service_bus_shared::bcl::bcl_date_time_utils::to_rfc3339(self)
    }
}

pub fn check_flags(app: &AppContext) -> Result<(), tonic::Status> {
    if !app.is_initialized() {
        return Err(tonic::Status::cancelled(
            "Application is not initialized yet",
        ));
    }

    if app.is_shutting_down() {
        return Err(tonic::Status::cancelled("Shutting down"));
    }

    Ok(())
}
