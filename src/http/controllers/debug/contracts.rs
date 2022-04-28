use my_http_server_swagger::MyHttpInput;

#[derive(MyHttpInput)]
pub struct GetDebugPageContract {
    #[http_query(name = "topicId"; description="Id of topic")]
    pub topic_id: String,
    #[http_query(name = "pageId"; description="Id of page")]
    pub page_id: i64,
}
