pub type BizCode = i32;
pub type ErrorTypeCode = i32;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct BizError {
    #[serde(rename = "BizCode")]
    pub biz_code: BizCode,
    #[serde(rename = "ErrorTypeCode")]
    pub error_type_code: ErrorTypeCode,
    #[serde(rename = "Message")]
    pub message: String,
}
