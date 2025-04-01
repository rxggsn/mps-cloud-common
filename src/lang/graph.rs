use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Debug,
};

use super::vec::{map_self, map_self_mut};

macro_rules! deep_transverse_from_apex {
    ($self:ident,$apex:ident,$group:ident,$operation:ident,$f:ident) => {
        let mut visited: HashSet<u16> = HashSet::new();
        let mut candidates = Vec::new();
        candidates.push(*$apex);
        while let Some(current_id) = candidates.pop() {
            if let Some(adjacents) = $self.edges.get(&current_id) {
                let mut filterd = adjacents
                    .iter()
                    .filter(|adjacent| !visited.contains(*adjacent))
                    .peekable();
                if filterd.peek().is_some() {
                    candidates.push(current_id);
                }
                filterd.for_each(|id| {
                    candidates.push(*id);
                });
            }

            if let Some(node) = $group.$operation(&current_id) {
                if !visited.contains(&current_id) {
                    $f(node);
                    visited.insert(current_id);
                }
            }
        }
    };
}

pub trait Node: serde::Serialize + serde::de::DeserializeOwned + Debug + Clone + Default {
    fn id(&self) -> u16;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct Graph<N> {
    pub nodes: Vec<N>,
    pub edges: HashMap<u16, Vec<u16>>,
    #[serde(skip)]
    apexes: Vec<u16>,
}

impl<N> Graph<N>
where
    N: Node,
{
    pub fn new(nodes: Vec<N>, edges: HashMap<u16, Vec<u16>>) -> Self {
        Self {
            nodes,
            edges,
            apexes: vec![],
        }
    }
    pub fn has_adjacent(&self, id: u16) -> bool {
        self.edges.contains_key(&id)
    }

    pub fn get_adjacent_ids(&self, id: &u16) -> Option<&Vec<u16>> {
        self.edges.get(id)
    }

    pub fn deep_traverse_mut<F: FnMut(&mut N)>(&mut self, mut f: F) {
        self.apexes = self.get_graph_apexes();
        let ref mut group = map_self_mut(&mut self.nodes, |node| node.id());

        self.apexes.iter().for_each(|apex| {
            let f = &mut f;
            deep_transverse_from_apex!(self, apex, group, get_mut, f);
        });
    }

    pub fn deep_traverse<F: FnMut(&N)>(&mut self, mut f: F) {
        self.apexes = self.get_graph_apexes();
        let ref group = map_self(&self.nodes, |node| node.id());

        self.apexes.iter().for_each(|apex| {
            let f = &mut f;
            deep_transverse_from_apex!(self, apex, group, get, f);
        });
    }

    pub fn get_graph_apexes(&self) -> Vec<u16> {
        if self.apexes.is_empty() {
            let mut candidates = HashSet::new();
            self.edges.iter().for_each(|(id, adjacent)| {
                candidates.insert(*id);
                adjacent.iter().for_each(|adjacent_id| {
                    candidates.insert(*adjacent_id);
                });
            });
            self.edges.iter().for_each(|(_, adjacent)| {
                adjacent.iter().for_each(|adjacent_id| {
                    candidates.remove(adjacent_id);
                });
            });
            let mut apexes: Vec<_> = candidates.into_iter().collect();
            apexes.sort();
            apexes
        } else {
            self.apexes.clone()
        }
    }

    pub fn deep_traverse_fold<F: FnMut(&mut N, &mut N)>(&mut self, mut f: F) {
        let mut init = Default::default();
        let apexes = self.get_graph_apexes();
        self.deep_traverse_mut(|node| {
            if apexes.contains(&node.id()) {
                init = Default::default();
            }
            f(&mut init, node);

            if init.id() != node.id() {
                init = node.clone();
            }
        });
    }

    pub fn list_nodes(&self, path: &[u16]) -> Vec<&N> {
        path.iter()
            .filter(|id| self.nodes.iter().find(|node| node.id() == **id).is_some())
            .map(|id| self.nodes.iter().find(|node| node.id() == *id).unwrap())
            .collect()
    }

    pub fn deep_traverse_from_apex<F: FnMut(&N)>(&self, apex_id: u16, mut f: F) {
        let ref group = map_self(&self.nodes, |node| node.id());
        let apex = &apex_id;
        deep_transverse_from_apex!(self, apex, group, get, f);
    }

    pub fn update_apexes<F: Fn(&mut N)>(&mut self, f: F) {
        self.nodes.iter_mut().for_each(|apex| {
            f(apex);
        });
    }

    pub fn remove_nodes(&mut self, ids: &[u16]) {
        let mut removed = HashSet::new();
        ids.iter().for_each(|id| {
            removed.insert(*id);
        });
        self.edges.iter_mut().for_each(|(_, adjacent)| {
            adjacent.retain(|id| !removed.contains(id));
        });
        self.edges
            .retain(|id, adjacent| !adjacent.is_empty() && !removed.contains(id));
        self.nodes.retain(|node| !removed.contains(&node.id()));
        self.apexes.clear();
        self.apexes = self.get_graph_apexes();
    }

    pub fn breadth_traverse_mut<F: FnMut(&mut N)>(&mut self, mut f: F) {
        let apexes = self.get_graph_apexes();
        let mut queue = VecDeque::new();
        let mut group = map_self_mut(&mut self.nodes, |node| node.id());
        apexes.iter().for_each(|apex| {
            queue.push_back(*apex);
        });

        while let Some(id) = queue.pop_front() {
            if let Some(node) = group.get_mut(&id) {
                f(node);
            }
            if let Some(adjacents) = self.edges.get(&id) {
                adjacents.iter().for_each(|adjacent| {
                    queue.push_back(*adjacent);
                });
            }
        }
    }

    pub fn get_node_mut(&mut self, id: &u16) -> Option<&mut N> {
        self.nodes.iter_mut().find(|node| node.id() == *id)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
    struct TestNode {
        id: u16,
        content: String,
    }

    impl super::Node for TestNode {
        fn id(&self) -> u16 {
            self.id
        }
    }

    type TestGraph = super::Graph<TestNode>;
    #[test]
    fn test_deep_traverse() {
        let mut graph = TestGraph::default();
        for idx in 1..=20 {
            graph.nodes.push(TestNode {
                id: idx,
                content: format!("content_{}", idx),
            });
        }

        graph.edges = HashMap::from_iter([
            (1, vec![2, 3]),
            (2, vec![4, 5, 6]),
            (3, vec![7, 8, 9, 10]),
            (4, vec![11, 12, 13]),
            (5, vec![14, 15]),
            (6, vec![16]),
            (7, vec![17, 18, 19]),
            (8, vec![20]),
        ]);

        let mut results = Vec::new();

        graph.deep_traverse(|node| {
            results.push(node.content.clone());
        });
        println!("{:?}", results);
        assert_eq!(results.len(), 20);
        assert_eq!(results[0], "content_1");
        assert_eq!(results[1], "content_3");
        assert_eq!(results[2], "content_10");
        assert_eq!(results[3], "content_9");
        assert_eq!(results[4], "content_8");
        assert_eq!(results[5], "content_20");
        assert_eq!(results[6], "content_7");
        assert_eq!(results[7], "content_19");
        assert_eq!(results[8], "content_18");
        assert_eq!(results[9], "content_17");
        assert_eq!(results[10], "content_2");
        assert_eq!(results[11], "content_6");
        assert_eq!(results[12], "content_16");
        assert_eq!(results[13], "content_5");
        assert_eq!(results[14], "content_15");
        assert_eq!(results[15], "content_14");
        assert_eq!(results[16], "content_4");
        assert_eq!(results[17], "content_13");
        assert_eq!(results[18], "content_12");
        assert_eq!(results[19], "content_11");
    }

    #[test]
    fn test_get_apexes() {
        let mut graph = TestGraph::default();
        for idx in 1..=20 {
            graph.nodes.push(TestNode {
                id: idx,
                content: format!("content_{}", idx),
            });
        }

        graph.edges = HashMap::from_iter([
            (1, vec![2, 3]),
            (2, vec![4, 5, 6]),
            (3, vec![7, 9, 10]),
            (4, vec![11]),
            (5, vec![14, 15]),
            (6, vec![16]),
            (7, vec![17, 18, 19]),
            (8, vec![12, 13]),
            (9, vec![20]),
        ]);

        let apexes = graph.get_graph_apexes();
        println!("{:?}", apexes);
        assert_eq!(apexes.len(), 2);
        assert_eq!(apexes[0], 1);
        assert_eq!(apexes[1], 8);
    }

    #[test]
    fn test_deep_traverse_mut() {
        let mut graph = TestGraph::default();
        for idx in 1..=20 {
            graph.nodes.push(TestNode {
                id: idx,
                content: format!("content_{}", idx),
            });
        }

        graph.edges = HashMap::from_iter([
            (1, vec![2, 3]),
            (2, vec![4, 5, 6]),
            (3, vec![7, 9, 10]),
            (4, vec![11]),
            (5, vec![14, 15]),
            (6, vec![16]),
            (7, vec![17, 18, 19]),
            (8, vec![12, 13]),
            (9, vec![20]),
        ]);

        graph.deep_traverse_mut(|node| {
            node.content = format!("new_content_{}", node.id);
        });

        let mut results = Vec::new();
        graph.deep_traverse(|node| {
            results.push(node.content.clone());
        });
        assert_eq!(results.len(), 20);
        assert_eq!(results[0], "new_content_1");
        assert_eq!(results[1], "new_content_3");
        assert_eq!(results[2], "new_content_10");
        assert_eq!(results[3], "new_content_9");
        assert_eq!(results[4], "new_content_20");
        assert_eq!(results[5], "new_content_7");
        assert_eq!(results[6], "new_content_19");
        assert_eq!(results[7], "new_content_18");
        assert_eq!(results[8], "new_content_17");
        assert_eq!(results[9], "new_content_2");
        assert_eq!(results[10], "new_content_6");
        assert_eq!(results[11], "new_content_16");
        assert_eq!(results[12], "new_content_5");
        assert_eq!(results[13], "new_content_15");
        assert_eq!(results[14], "new_content_14");
        assert_eq!(results[15], "new_content_4");
        assert_eq!(results[16], "new_content_11");
        assert_eq!(results[17], "new_content_8");
        assert_eq!(results[18], "new_content_13");
        assert_eq!(results[19], "new_content_12");
    }

    #[test]
    fn test_deep_traverse_fold() {
        let mut graph = TestGraph::default();
        for idx in 1..=20 {
            graph.nodes.push(TestNode {
                id: idx,
                content: format!("content_{}", idx),
            });
        }

        graph.edges = HashMap::from_iter([
            (1, vec![2, 3]),
            (2, vec![4, 5, 6]),
            (3, vec![7, 9, 10]),
            (4, vec![11]),
            (5, vec![14, 15]),
            (6, vec![16]),
            (7, vec![17, 18, 19]),
            (8, vec![12, 13]),
            (9, vec![20]),
        ]);

        let mut content = "".to_string();
        graph.deep_traverse_fold(|parent, current| {
            if content != "" {
                content.push_str("\nand\n");
                content.push_str(&current.content);
            } else {
                content.push_str(&current.content);
            }
            if !parent.content.is_empty() {
                current.content = format!("{}\nand\n{}", parent.content, current.content);
            }
        });
        println!("{}", content);
        assert_eq!(content, "content_1\nand\ncontent_3\nand\ncontent_10\nand\ncontent_9\nand\ncontent_20\nand\ncontent_7\nand\ncontent_19\nand\ncontent_18\nand\ncontent_17\nand\ncontent_2\nand\ncontent_6\nand\ncontent_16\nand\ncontent_5\nand\ncontent_15\nand\ncontent_14\nand\ncontent_4\nand\ncontent_11\nand\ncontent_8\nand\ncontent_13\nand\ncontent_12");
    }
}
