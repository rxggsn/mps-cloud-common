use std::fmt::Display;

pub struct MarkdownTable {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl MarkdownTable {
    pub fn new<T: Display>(headers: &[T]) -> Self {
        Self {
            headers: headers.iter().map(|h| h.to_string()).collect(),
            rows: Vec::new(),
        }
    }

    pub fn add_row(&mut self, row: Vec<String>) -> &mut Self {
        self.rows.push(row);
        self
    }

    pub fn with_rows(&mut self, vec: Vec<Vec<String>>) -> &mut Self {
        self.rows.extend(vec);
        self
    }
}

impl Display for MarkdownTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let header = self.headers.join(" | ");
        let header = format!("| {} |", header);
        let header = format!(
            "{}\n{}",
            header,
            format!(
                "| {} |",
                self.headers
                    .iter()
                    .map(|_| "---")
                    .collect::<Vec<_>>()
                    .join(" | ")
            )
        );
        let rows = self
            .rows
            .iter()
            .map(|row| {
                let row = row.join(" | ");
                format!("| {} |", row)
            })
            .collect::<Vec<_>>()
            .join("\n");

        write!(f, "{}\n{}", header, rows)
    }
}

#[cfg(test)]
mod tests {
    use super::MarkdownTable;

    #[test]
    fn test_markdown_table_to_string() {
        assert_eq!(
            MarkdownTable::new(&["a", "b", "c"])
                .add_row(vec!["1".to_string(), "2".to_string(), "3".to_string()])
                .add_row(vec!["4".to_string(), "5".to_string(), "6".to_string()])
                .add_row(vec!["7".to_string(), "8".to_string(), "9".to_string()])
                .to_string(),
            "| a | b | c |\n| --- | --- | --- |\n| 1 | 2 | 3 |\n| 4 | 5 | 6 |\n| 7 | 8 | 9 |"
        );
    }
}
