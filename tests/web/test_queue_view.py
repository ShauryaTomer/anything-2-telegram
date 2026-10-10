"""Exercise queue refresh without the browser-specific global window.event."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest


def test_queue_refresh_and_sort_changes_do_not_require_global_event() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is required to execute the queue script")
    template = (
        Path(__file__).parents[2]
        / "anything2telegram/web/templates/page.html"
    ).read_text()
    start = template.index("  function applyQueueView(")
    end = template.index("  stateFilter.addEventListener", start)
    script = template[start:end]
    result = subprocess.run(
        [node, "-"],
        input="""
const assert = require('node:assert/strict');
const vm = require('node:vm');
function row(title, order, state) {
  return { dataset: { title, order, state, progress: order },
    nextElementSibling: null, hidden: false };
}
const first = row('Zulu', 0, 'failed');
const child = { hidden: false,
  classList: { contains: (name) => name === 'queue-child' },
  nextElementSibling: null };
first.nextElementSibling = child;
const second = row('Alpha', 1, 'queued');
const body = {
  rows: [first, child, second],
  querySelectorAll() { return this.rows.filter((r) => r.dataset); },
  append(...rows) {
    this.rows = this.rows.filter((r) => !rows.includes(r)).concat(rows);
  },
};
const context = vm.createContext({
  document: { querySelector: () => body },
  stateFilter: { value: 'all' },
  queueSort: { value: 'queue' },
  activeStates: new Set(['queued']),
  attentionStates: new Set(['failed']),
  filterEmpty: { hidden: true },
});
""" + "vm.runInContext(" + json.dumps(script) + ", context);\n" + """
// Initial load and programmatic refresh: no global event in this context.
vm.runInContext('applyQueueView()', context);
assert.deepEqual(body.rows, [first, child, second]);

// Explicit dropdown event sorts each parent together with its children.
context.queueSort.value = 'name';
vm.runInContext('applyQueueView({ target: queueSort })', context);
assert.deepEqual(body.rows, [second, first, child]);
vm.runInContext('applyQueueView()', context);
assert.deepEqual(body.rows, [second, first, child]);

context.queueSort.value = 'queue';
vm.runInContext('applyQueueView({ target: queueSort })', context);
assert.deepEqual(body.rows, [first, child, second]);

context.stateFilter.value = 'active';
vm.runInContext('applyQueueView({ target: stateFilter })', context);
assert.equal(first.hidden, true);
assert.equal(child.hidden, true);
assert.equal(second.hidden, false);
assert.equal(context.filterEmpty.hidden, true);

// HTMX refresh passes an event unrelated to the sorting dropdown.
vm.runInContext('applyQueueView({ target: document })', context);
assert.deepEqual(body.rows, [first, child, second]);
""",
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
