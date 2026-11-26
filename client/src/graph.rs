// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;

use kat0_pipeline_protocol::model::{Definition, Snapshot};
use leptos::{html, prelude::*};
use leptos_node_graph::{
    EditorRegistry,
    registry::{ConnectionEntry, PortEntry},
    types::{EditorConfig, PortDirection, PortId, PortType, Position},
    utils::rounded_polyline_path,
};
use leptos_use::{UseElementSizeReturn, use_element_size};

use crate::components::{JobButton, Tab};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Port(usize, PortDirection);

impl PortId for Port {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Flow;

impl PortType for Flow {
    fn compatible(_: &Self, _: &Self) -> bool {
        false
    }
    fn type_id(&self) -> String {
        "flow".into()
    }
    fn from_type_id(_: &str) -> Self {
        Self
    }
}

pub(crate) fn pipeline(
    definition: &Definition,
    snapshot: ReadSignal<Snapshot>,
    selected: RwSignal<Option<String>>,
) -> impl IntoView + use<> {
    // Keep the library's topology and ports, but draw the report's read-only routes
    // without the editor's fixed-radius turns or interactive controls.
    let registry = EditorRegistry::<usize, Port, usize, Flow>::new(
        EditorConfig::default(),
        Callback::new(|_| {}),
    );
    let node_ids: HashMap<_, _> = definition
        .jobs()
        .enumerate()
        .map(|(index, job)| (job.id.as_str(), index))
        .collect();
    let connections_map: HashMap<_, _> = definition
        .jobs()
        .flat_map(|job| {
            job.dependencies
                .iter()
                .map(move |from| (from.as_str(), job.id.as_str()))
        })
        .enumerate()
        .map(|(id, (from, to))| {
            (
                id,
                ConnectionEntry {
                    id,
                    source: Port(node_ids[from], PortDirection::Output),
                    target: Port(node_ids[to], PortDirection::Input),
                },
            )
        })
        .collect();
    let connection_count = connections_map.len();
    let connections = registry.connections;
    connections.set(connections_map);
    let ports = registry.ports;

    let board = NodeRef::<html::Div>::new();
    let nodes: Vec<_> = definition
        .jobs()
        .map(|_| NodeRef::<html::Button>::new())
        .collect();
    let column_style = format!("--column-count: {}", definition.columns.len());
    let columns = definition.columns.iter().enumerate().map(|(index, column)| {
        let heading_id = format!("{}-column-{index}", Tab::Pipeline.panel_id());
        view! {
            <section class="stage-column" aria-labelledby=heading_id>
                <h2 id=heading_id.clone()>{column.title.clone()}</h2>
                <ul class="job-list" role="list">
                    {column.jobs.iter().map(|job| {
                        let node = nodes[node_ids[job.id.as_str()]];
                        let job_id = StoredValue::new(job.id.clone());
                        view! { <li><JobButton snapshot selected tab=Tab::Pipeline job_id title=job.title.clone() node/></li> }
                    }).collect_view()}
                </ul>
            </section>
        }
    }).collect_view();
    let UseElementSizeReturn { width, height } = use_element_size(board);
    Effect::new(move |_| {
        width.track();
        height.track();
        let Some(board) = board.get() else {
            return;
        };
        let board = board.get_bounding_client_rect();
        if board.width() == 0.0 {
            return;
        }

        let mut measured = HashMap::new();
        for (id, node) in nodes.iter().enumerate() {
            let Some(button) = node.get() else {
                return;
            };
            let button = button.get_bounding_client_rect();
            let x = button.x() - board.x();
            let y = button.y() - board.y();
            let width = button.width();
            let height = button.height();
            for (direction, edge) in [
                (PortDirection::Input, x),
                (PortDirection::Output, x + width),
            ] {
                let port = Port(id, direction);
                measured.insert(
                    port,
                    PortEntry {
                        id: port,
                        node_id: id,
                        direction,
                        port_type: Flow,
                        position: Position::new(edge, y + height / 2.0),
                        slot_index: 0,
                        offset: None,
                    },
                );
            }
        }
        ports.set(measured);
    });

    view! {
        <div class="pipeline-scroll" role="region" aria-label="Pipeline stages" tabindex="0">
            <div class="pipeline" style=column_style node_ref=board>
                <svg class="connections" aria-hidden="true" focusable="false">
                    {(0..connection_count).map(|id| view! {
                        <path data-connection=id d=move || connections.with(|connections| {
                            let connection = connections.get(&id)?;
                            ports.with(|ports| {
                                let source = ports.get(&connection.source)?.position;
                                let target = ports.get(&connection.target)?.position;
                                Some(connection_path(source, target))
                            })
                        })/>
                    }).collect_view()}
                </svg>
                {columns}
            </div>
        </div>
    }
}

fn connection_path(source: Position, target: Position) -> String {
    let dy = target.y - source.y;
    let radius = ((target.x - source.x) / 2.0).min(dy.abs() / 2.0);
    if radius <= 0.0 {
        return rounded_polyline_path(&[source, target], 0.0);
    }

    // Fit two quarter circles between the ports and leave a vertical run in the middle.
    let mid_x = source.x.midpoint(target.x);
    let turn_y = radius.copysign(dy);
    let sweep = u8::from(dy > 0.0);
    format!(
        "M {sx} {sy} H {out_x} A {radius} {radius} 0 0 {sweep} {mid_x} {out_y} \
         V {in_y} A {radius} {radius} 0 0 {reverse_sweep} {in_x} {ty} H {tx}",
        sx = source.x,
        sy = source.y,
        out_x = mid_x - radius,
        out_y = source.y + turn_y,
        in_y = target.y - turn_y,
        reverse_sweep = 1 - sweep,
        in_x = mid_x + radius,
        tx = target.x,
        ty = target.y,
    )
}
