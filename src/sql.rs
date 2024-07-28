// NOTE:this might be useless
pub enum Statement {
    SelectStatement(SelectStatement),
    CreateTableStatement(CreateTableStatement),
}

pub struct CreateTableStatement {
    pub table_name: String,
    // todo
}

/// Simple representation of a SQL SELECT statement
/// ```sql
/// SELECT name, color FROM apples WHERE color='blue';
/// ```
/// will be parsed into:
/// ```rust
/// SelectStatement {
///    selectables: vec![Selectable::Column("name"), Selectable::Column("color")],
///    from_target: Targetable::TableOrView("apples"),
///    conditions: vec![Condition {column: "color", value: "blue"}]
/// }
/// ```
///
#[derive(Debug, PartialEq)]
pub struct SelectStatement {
    pub selectables: Vec<Selectable>,
    pub from_target: Targetable,
    pub conditions: Vec<Condition>,
}

/// Any column or COUNT(*) in a SELECT statement
/// ```sql
/// SELECT name, COUNT(*) FROM apples;
/// ```
/// will be parsed into:
/// ```rust
/// vec![Selectable::Column("name"), Selectable::CountStar]
/// ```
#[derive(Debug, PartialEq)]
pub enum Selectable {
    Column(String),
    CountStar,
}

/// Any table or view in a FROM clause
/// ```sql
/// SELECT name FROM apples;
/// ```
/// will be parsed into:
/// ```rust
/// Targetable::TableOrView("apples")
/// ```
#[derive(Debug, PartialEq)]
pub enum Targetable {
    TableOrView(String),
    Other(String),
}

/// This assumes we only have "column-value" string-string equality conditions for now
#[derive(Debug, PartialEq)]
pub struct Condition {
    pub column: String,
    pub value: String,
}

peg::parser! {
  pub grammar sql_query() for str {
    /// Parses a simple SELECT statement
    /// ```sql
    /// SELECT name, color FROM apples WHERE color='blue';
    /// ```
    /// will be parsed into:
    /// ```rust
    /// SelectStatement {
    ///   selectables: vec![Selectable::Column("name"), Selectable::Column("color")],
    ///   from_target: Targetable::TableOrView("apples"),
    ///   conditions: vec![Condition {column: "color", value: "blue"}]
    ///   }
    /// ```
    pub rule select_statement() -> SelectStatement
        = select() _+ selectables:(selectable() ++ [',' | _] ) _+ from()
        _+ from_target:targetable() _+ where() _+ conditions:(condition() ++ and())
        {SelectStatement{selectables, from_target, conditions}}

    rule _() = quiet!{[' ' | '\n' | '\t']+}

    rule select() -> ()
        = ( "select" / "SELECT")

    rule selectable() -> Selectable
        = s:(count_star() / column()) {s}

    rule count_star() -> Selectable
        = "*" {Selectable::CountStar {}}

    rule column() -> Selectable
        = name:identifier() {Selectable::Column(name.to_string())}

    rule from() -> ()
        = ("from" / "FROM")

    rule targetable() -> Targetable
        = name:identifier() {Targetable::TableOrView(name)}

    rule identifier() -> String
        = name:$(['a'..='z' | 'A'..='Z' | '_']+) {name.to_string()}

    rule where() -> ()
        = ("where" / "WHERE")

    rule condition() -> Condition
        = column:identifier() _+ "=" +_ value:string_litteral() {Condition {column, value}}

    rule string_litteral() -> String
        = "'" s:$(['\'']*) {s.to_string()}

    rule and() -> ()
        = ("and" / "AND")

  }
}

#[cfg(test)]
mod test {
    use crate::sql::{Condition, SelectStatement, Selectable, Targetable};

    use super::sql_query;

    #[test]
    fn parse_count_star_select_query() {
        let result = sql_query::select_statement("SELECT COUNT(*) FROM apples WHERE color='blue'");
        assert_eq!(
            result,
            Ok(SelectStatement {
                selectables: vec![Selectable::CountStar],
                from_target: Targetable::TableOrView(String::from("apples")),
                conditions: vec![Condition {
                    column: String::from("color"),
                    value: String::from("blue")
                }]
            })
        )
    }

    #[test]
    fn parse_simple_select_query() {
        let result =
            sql_query::select_statement("SELECT name, color FROM apples WHERE color='blue'");
        assert_eq!(
            result,
            Ok(SelectStatement {
                selectables: vec![
                    Selectable::Column(String::from("name")),
                    Selectable::Column(String::from("color"))
                ],
                from_target: Targetable::TableOrView(String::from("apples")),
                conditions: vec![Condition {
                    column: String::from("color"),
                    value: String::from("blue")
                }]
            })
        )
    }
}
