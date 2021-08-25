use std::{collections::HashMap, str::FromStr};

use super::HttpFailResult;

pub struct QueryString {
    query_string: HashMap<String, String>,
}

impl QueryString {
    pub fn new(src: Option<&str>) -> Self {
        let mut result = Self {
            query_string: HashMap::new(),
        };

        if let Some(src) = src {
            super::url_utils::parse_query_string(&mut result.query_string, src);
        }

        return result;
    }

    pub fn get_required_query_parameter<'r, 't>(
        &'r self,
        name: &'t str,
    ) -> Result<&'r String, HttpFailResult> {
        let result = self.query_string.get(name);

        match result {
            Some(e) => Ok(e),
            None => Err(HttpFailResult::query_parameter_requires(name)),
        }
    }

    pub fn get_query_parameter_or_defaul<T: FromStr>(&self, name: &str, default: T) -> T {
        match self.query_string.get(name) {
            Some(msg) => match msg.parse::<T>() {
                Ok(result) => result,
                _ => default,
            },
            None => default,
        }
    }
}
