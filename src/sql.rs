// NOTE:this might be useless
pub enum Statement {
    SelectStatement(SelectStatement),
    CreateTableStatement(CreateTableStatement),
}

pub struct CreateTableStatement {
    pub table_name: String,
    // todo
}

#[derive(Debug, PartialEq)]
pub struct SelectStatement {
    pub selectables: Vec<Selectable>,
    pub from_target: Targetable,
    pub conditions: Vec<Condition>,
}

#[derive(Debug, PartialEq)]
enum Selectable {
    Column(String),
    CountStar,
}

#[derive(Debug, PartialEq)]
enum Targetable {
    TableOrView(String),
    Other(String),
}

/// This assumes we only have "column-value" string-string equality conditions for now
#[derive(Debug, PartialEq)]
struct Condition {
    pub column: String,
    pub value: String,
}

peg::parser! {
  grammar sql_query() for str {
    // SELECT COUNT(*) FROM apples WHERE color='blue';
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
