#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltInModelCatalogItem {
    pub api_id: &'static str,
    pub policy_id: &'static str,
    pub display_name: &'static str,
    pub created: i64,
    pub max_tokens: i32,
}

const BUILT_IN_MODEL_CATALOG: [BuiltInModelCatalogItem; 12] = [
    BuiltInModelCatalogItem {
        api_id: "claude-opus-4-7",
        policy_id: "claude-opus-4.7",
        display_name: "Claude Opus 4.7",
        created: 1760486400,
        max_tokens: 64000,
    },
    BuiltInModelCatalogItem {
        api_id: "claude-opus-4-7-thinking",
        policy_id: "claude-opus-4.7-thinking",
        display_name: "Claude Opus 4.7 (Thinking)",
        created: 1760486400,
        max_tokens: 64000,
    },
    BuiltInModelCatalogItem {
        api_id: "claude-opus-4-6",
        policy_id: "claude-opus-4.6",
        display_name: "Claude Opus 4.6",
        created: 1754265600,
        max_tokens: 32000,
    },
    BuiltInModelCatalogItem {
        api_id: "claude-opus-4-6-thinking",
        policy_id: "claude-opus-4.6-thinking",
        display_name: "Claude Opus 4.6 (Thinking)",
        created: 1754265600,
        max_tokens: 32000,
    },
    BuiltInModelCatalogItem {
        api_id: "claude-opus-4-5-20251101",
        policy_id: "claude-opus-4.5-20251101",
        display_name: "Claude Opus 4.5",
        created: 1761955200,
        max_tokens: 32000,
    },
    BuiltInModelCatalogItem {
        api_id: "claude-opus-4-5-20251101-thinking",
        policy_id: "claude-opus-4.5-20251101-thinking",
        display_name: "Claude Opus 4.5 (Thinking)",
        created: 1761955200,
        max_tokens: 32000,
    },
    BuiltInModelCatalogItem {
        api_id: "claude-sonnet-4-6",
        policy_id: "claude-sonnet-4.6",
        display_name: "Claude Sonnet 4.6",
        created: 1754265600,
        max_tokens: 64000,
    },
    BuiltInModelCatalogItem {
        api_id: "claude-sonnet-4-6-thinking",
        policy_id: "claude-sonnet-4.6-thinking",
        display_name: "Claude Sonnet 4.6 (Thinking)",
        created: 1754265600,
        max_tokens: 64000,
    },
    BuiltInModelCatalogItem {
        api_id: "claude-sonnet-4-5-20250929",
        policy_id: "claude-sonnet-4.5-20250929",
        display_name: "Claude Sonnet 4.5",
        created: 1759104000,
        max_tokens: 64000,
    },
    BuiltInModelCatalogItem {
        api_id: "claude-sonnet-4-5-20250929-thinking",
        policy_id: "claude-sonnet-4.5-20250929-thinking",
        display_name: "Claude Sonnet 4.5 (Thinking)",
        created: 1759104000,
        max_tokens: 64000,
    },
    BuiltInModelCatalogItem {
        api_id: "claude-haiku-4-5-20251001",
        policy_id: "claude-haiku-4.5-20251001",
        display_name: "Claude Haiku 4.5",
        created: 1760486400,
        max_tokens: 64000,
    },
    BuiltInModelCatalogItem {
        api_id: "claude-haiku-4-5-20251001-thinking",
        policy_id: "claude-haiku-4.5-20251001-thinking",
        display_name: "Claude Haiku 4.5 (Thinking)",
        created: 1760486400,
        max_tokens: 64000,
    },
];

pub fn built_in_model_catalog() -> &'static [BuiltInModelCatalogItem] {
    &BUILT_IN_MODEL_CATALOG
}
