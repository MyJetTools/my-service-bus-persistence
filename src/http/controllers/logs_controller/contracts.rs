use my_http_server_swagger::MyHttpInput;

#[derive(Debug, MyHttpInput)]
pub struct GetLogsByTopicHttpInput {
    #[http_path(name = "topicId"; description = "Id of Topic")]
    pub topic_id: String,
}
