// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        let mut level_sizes = Vec::new();
        level_sizes.push(snapshot.l0_sstables.len());
        for (_, files) in snapshot.levels.iter() {
            level_sizes.push(files.len());
        }

        for i in 0..self.options.max_levels {
            if i == 0
                && snapshot.l0_sstables.len() < self.options.level0_file_num_compaction_trigger
            {
                continue;
            }
            let lower_level = i + 1;
            let size_ratio = level_sizes[lower_level] as f64 / level_sizes[i] as f64;
            if size_ratio < self.options.size_ratio_percent as f64 / 100.0 {
                println!(
                    "compaction triggered at level {} and {} with size ratio {}",
                    i, lower_level, size_ratio
                );
                return Some(SimpleLeveledCompactionTask {
                    upper_level: if i == 0 { None } else { Some(i) },
                    upper_level_sst_ids: if i == 0 {
                        snapshot.l0_sstables.clone()
                    } else {
                        snapshot.levels[i - 1].1.clone()
                    },
                    lower_level,
                    lower_level_sst_ids: snapshot.levels[lower_level - 1].1.clone(),
                    is_lower_level_bottom_level: lower_level == self.options.max_levels,
                });
            }
        }
        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut files_to_remove = Vec::new();

        // ─── 1) Filter out only the SSTs we compacted from the upper side ───
        match task.upper_level {
            Some(level) => {
                let old = &snapshot.levels[level - 1].1;
                // sanity: each chosen SST must exist in that level
                for id in &task.upper_level_sst_ids {
                    assert!(old.contains(id), "upper-level SST ID {} not found", id);
                }
                // schedule those for removal
                files_to_remove.extend(&task.upper_level_sst_ids);
                // keep everything else in that level
                snapshot.levels[level - 1].1 = old
                    .iter()
                    .copied()
                    .filter(|id| !task.upper_level_sst_ids.contains(id))
                    .collect();
            }
            None => {
                let old = &snapshot.l0_sstables;
                for id in &task.upper_level_sst_ids {
                    assert!(old.contains(id), "L0 SST ID {} not found", id);
                }
                files_to_remove.extend(&task.upper_level_sst_ids);
                snapshot.l0_sstables = old
                    .iter()
                    .copied()
                    .filter(|id| !task.upper_level_sst_ids.contains(id))
                    .collect();
            }
        }

        // ─── 2) Filter out only the SSTs we compacted from the lower side, then append new ones ───
        {
            let lvl = task.lower_level - 1;
            let old = &snapshot.levels[lvl].1;
            for id in &task.lower_level_sst_ids {
                assert!(old.contains(id), "lower-level SST ID {} not found", id);
            }
            files_to_remove.extend(&task.lower_level_sst_ids);

            // everything we didn’t compact *plus* the newly built SST IDs
            let mut new_list: Vec<usize> = old
                .iter()
                .copied()
                .filter(|id| !task.lower_level_sst_ids.contains(id))
                .collect();
            new_list.extend_from_slice(output);
            snapshot.levels[lvl].1 = new_list;
        }

        (snapshot, files_to_remove)
    }
}
