use my_http_server::macros::MyHttpInput;

#[derive(MyHttpInput)]
pub struct DeleteTopicHttpContract {
    #[http_query(name = "topicId"; description="Id of topic")]
    pub topic_id: String,

    #[http_query(name = "apiKey"; description="Api Key")]
    pub api_key: String,

    #[http_query(name = "deleteAfter"; description="GC moment in RFC3339 (optional)"; default: "")]
    pub delete_after: Option<String>,
}
