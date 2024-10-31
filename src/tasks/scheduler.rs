use std::collections::BinaryHeap;

use crate::tasks::Task;

pub trait TaskQueue<T: Task> {
    fn add_task(&mut self, task: T);
    fn pop_task(&mut self) -> Option<T>;
    fn has_task(&self) -> bool {
        !self.is_empty()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize;
}

pub struct LocalPriorityTaskQueue<T: Task> {
    task: BinaryHeap<T>,
}

impl<T: Task> LocalPriorityTaskQueue<T> {
    pub fn new() -> Self {
        Self {
            task: BinaryHeap::new(),
        }
    }

}

impl<T: Task> TaskQueue<T> for LocalPriorityTaskQueue<T> {
    fn add_task(&mut self, task: T) {
        self.task.push(task);
    }

    fn pop_task(&mut self) -> Option<T> {
        self.task.pop()
    }

    fn len(&self) -> usize {
        self.task.len()
    }
}