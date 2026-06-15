//! Sort direction shared by the read-model list queries.
//!
//! Read queries never interpolate a caller-supplied direction string into SQL.
//! The API layer parses the request into this closed enum (rejecting anything
//! else as a 400) and the read fn maps it to a fixed `ASC`/`DESC` keyword, so
//! ordering can never become a SQL-injection vector while still being driven by
//! the request.

/// Ascending or descending order for a list query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

impl SortDirection {
    /// Parse the public `order_direction` query param: case-insensitive,
    /// defaulting to ascending when absent. Returns `None` for unrecognised
    /// values so the API layer can answer 400 with the offending value.
    pub fn from_api_param(value: Option<&str>) -> Option<Self> {
        match value.unwrap_or("asc").to_ascii_lowercase().as_str() {
            "asc" => Some(Self::Asc),
            "desc" => Some(Self::Desc),
            _ => None,
        }
    }

    /// The SQL keyword for this direction. Always a fixed literal, never user
    /// input, so it is safe to interpolate into an `ORDER BY` clause.
    pub fn as_sql(self) -> &'static str {
        match self {
            Self::Asc => "ASC",
            Self::Desc => "DESC",
        }
    }

    /// The seek-cursor comparison operator for this direction: ascending pages
    /// forward (`>`), descending pages backward (`<`). Used by the keyset-paged
    /// transaction and event lists. A fixed literal, safe to interpolate.
    pub fn cursor_operator(self) -> &'static str {
        match self {
            Self::Asc => ">",
            Self::Desc => "<",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Parsing: default, case-insensitivity, and rejection of junk values.
    #[test]
    fn parses_direction_param_with_default_and_rejects_unknown() {
        assert_eq!(
            SortDirection::from_api_param(None),
            Some(SortDirection::Asc)
        );
        assert_eq!(
            SortDirection::from_api_param(Some("DESC")),
            Some(SortDirection::Desc)
        );
        assert_eq!(
            SortDirection::from_api_param(Some("asc")),
            Some(SortDirection::Asc)
        );
        assert_eq!(SortDirection::from_api_param(Some("sideways")), None);
    }

    // The SQL keyword must be exactly ASC/DESC (interpolated into ORDER BY).
    #[test]
    fn maps_to_sql_keyword() {
        assert_eq!(SortDirection::Asc.as_sql(), "ASC");
        assert_eq!(SortDirection::Desc.as_sql(), "DESC");
    }
}
