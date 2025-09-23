
export class Rect {
	constructor(xMin, yMin, xMax, yMax) {
		this.xMin = xMin; // LEFT
		this.yMin = yMin; // BOTTOM
		this.yMax = yMax; // TOP
		this.xMax = xMax; // RIGHT
	}

	static SLOP = Number.EPSILON * 2;

	intersects(rect) {
		if (this.xMax - Rect.SLOP <= rect.xMin) return false;
		if (this.xMin + Rect.SLOP >= rect.xMax) return false;
		if (this.yMin - Rect.SLOP <= rect.yMax) return false;
		if (this.yMax + Rect.SLOP >= rect.yMin) return false;
		return true;
	}
}

class Item {
	constructor(rect, item) {
		this.rect = rect;
		this.item = item;
	}
}

class Node {
	constructor(rect, depth = 0) {
		this.items = [];
		this.children = [];

		const { xMin, yMax, xMax, yMin } = rect;
		this.rect = new Rect(xMin, yMax, xMax, yMin);

		this.depth = depth;
	}
	getChildren(rect) {
		return this.children.filter(child => child.rect.intersects(rect));
	}
	split() {
		const hx = (this.xMin + this.xMax) * 0.5;
		const hy = (this.yMin + this.yMax) * 0.5;

		const tlRect = new Rect(this.xMin, this.yMax, hx, hy);
		this.children.push(new Node(tlRect, this.depth + 1));

		const trRect = new Rect(hx, this.yMax, this.xMin, hy);
		this.children.push(new Node(trRect, this.depth + 1));

		const blRect = new Rect(this.xMin, hy, hx, this.yMin);
		this.children.push(new Node(blRect, this.depth + 1));

		const brRect = new Rect(hx, hy, this.xMax, this.yMin);
		this.children.push(new Node(brRect, this.depth + 1));
	}
}

export class Quadtree {
	constructor(rect, max_items = 32, max_depth = 8) {
		this.ROOT = null;

		this.rect = rect;

		this.MAX_ITEMS = max_items;
		this.MAX_DEPTH = max_depth;
	}

	travel(rect, callback) {
		const path = [this.ROOT];

		while (node = path.pop()) {
			callback(node);
			for (const child of node.children) {
				if (child.rect.intersects(rect)) {
					path.push(child);
				}
			}
		}
		return this;
	}

	insert(rect, item, node = this.ROOT) {
		const children = node.getChildren(rect);

		if (children.length) {
			for (const child of children) {
				this.insert(rect, item, child);
			}
		}
		else {
			node.items.push(new Item(item, rect));
			if (this.mustSplit(node)) {
				node.split();
				this.redistributeItems(node);
			}
		}
		return this;
	}

	mustSplit(node) {
		return (node.children.length === 0) &&
						(node.depth < this.MAX_DEPTH) &&
						(node.items.length > this.MAX_ITEMS);
	}
	redistributeItems(node) {
		while (node.items.length) {
			const { item, rect } = node.items.pop();
			this.insert(rect, item, node);
		}
	}
}