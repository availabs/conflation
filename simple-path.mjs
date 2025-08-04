function StackSet() {
  this.set = new Set();
  this.stack = [];
  this.lengths = [];
}

StackSet.prototype.has = function (value) {
  return this.set.has(value);
};

StackSet.prototype.push = function (value, graph) {
  this.stack.push(value);
  this.set.add(value);
  if (this.stack.length === 1) {
    this.lengths.push(0);
  }
  else {
    const edgeKey = `${ this.stack.at(-2) }-${ this.stack.at(-1) }`;
    const length = graph.getEdgeAttribute(edgeKey, "length");
    this.lengths.push(length);
  }
};

StackSet.prototype.pop = function () {
  this.set.delete(this.stack.pop());
  this.lengths.pop();
};

StackSet.prototype.path = function (value) {
  return this.stack.concat(value);
};

StackSet.prototype.length = function () {
  return this.lengths.reduce((a, c) => {
    return a + c;
  }, 0);
};

StackSet.of = function (value, graph) {
  var set = new StackSet();

  set.push(value, graph);

  return set;
};

/**
 * Function returning all the paths between source & target in the graph.
 * 
 * @param  {Graph}   graph      - Target graph.
 * @param  {string}  source     - Source node.
 * @param  {string}  target     - Target node.
 * @param  {options} options    - Options:
 * @param  {number}  maxLength  - Max traversal length in miles (default: infinity).
 * @return {array}              - The found paths.
 * 
 **/
function allSimplePaths(graph, source, target, options) {

  if (!graph.hasNode(source))
    throw new Error(
      'graphology-simple-path.allSimplePaths: expecting: could not find source node "' +
        source +
        '" in the graph.'
    );

  if (!graph.hasNode(target))
    throw new Error(
      'graphology-simple-path.allSimplePaths: expecting: could not find target node "' +
        target +
        '" in the graph.'
    );

  options = options || {};
  var maxLength =
    typeof options.maxLength === 'number' ? options.maxLength : Infinity;

  source = '' + source;
  target = '' + target;

  var stack = [graph.outboundNeighbors(source)];
  var visited = StackSet.of(source, graph);

  var paths = [];
  var p;

  var children, child;

  while (stack.length !== 0) {
    children = stack[stack.length - 1];
    child = children.pop();

    if (!child) {
      stack.pop();
      visited.pop();
    }
    else {
      if (visited.has(child)) continue;

      if (child === target) {
        p = visited.path(child);
        paths.push(p);
      }

      visited.push(child, graph);

      if (!visited.has(target) && visited.length() < maxLength) {
        stack.push(graph.outboundNeighbors(child));
      }
      else {
        visited.pop();
      }
    }
  }

  return paths;
}

export default allSimplePaths;