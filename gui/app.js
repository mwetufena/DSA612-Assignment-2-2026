/* Mjölnir Distributed Food Delivery — UI logic */

// Backend ports (host-mapped via docker-compose)
const SERVICES = {
    customer:    "http://localhost:8081",
    restaurant:  "http://localhost:8082",
    order:       "http://localhost:8083",
    payment:     "http://localhost:8084",
    delivery:    "http://localhost:8085",
    notification:"http://localhost:8086",
    admin:       "http://localhost:8087"
};

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

function toast(msg, type = "info") {
    const el = $("#toast");
    el.textContent = msg;
    el.className = "toast show " + type;
    setTimeout(() => { el.className = "toast"; }, 3500);
}

async function api(base, path, opts = {}) {
    const url = base + path;
    const headers = { "Content-Type": "application/json", ...(opts.headers || {}) };
    try {
        const r = await fetch(url, { ...opts, headers });
        const text = await r.text();
        let data;
        try { data = text ? JSON.parse(text) : {}; } catch { data = text; }
        if (!r.ok) {
            toast((data && data.message) || `${r.status} ${r.statusText}`, "error");
            throw new Error(data?.message || r.statusText);
        }
        return data;
    } catch (e) {
        toast(`Network error: ${e.message}`, "error");
        throw e;
    }
}

// ----- Tabs -----
$$(".tab").forEach(btn => {
    btn.addEventListener("click", () => {
        $$(".tab").forEach(b => b.classList.remove("active"));
        $$(".panel").forEach(p => p.classList.remove("active"));
        btn.classList.add("active");
        const tab = btn.dataset.tab;
        $("#" + tab).classList.add("active");
        if (tab === "dashboard") checkHealth();
        if (tab === "customers") refreshCustomers();
        if (tab === "restaurants") refreshRestaurants();
        if (tab === "orders") refreshOrders();
        if (tab === "drivers") refreshDrivers();
        if (tab === "notifications") refreshNotifications();
        if (tab === "reports") refreshReports();
    });
});

// ----- Health -----
async function checkHealth() {
    const grid = $("#health-grid");
    grid.innerHTML = "";
    const entries = Object.entries(SERVICES).map(([name, base]) => {
        const card = document.createElement("div");
        card.className = "service-card";
        card.innerHTML = `
            <div>
                <div class="name">${name}</div>
                <div class="port">${base}</div>
            </div>
            <div class="indicator loading" id="dot-${name}"></div>`;
        grid.appendChild(card);
        return [name, base];
    });

    const status = $("#service-status");
    let allOk = true;
    await Promise.all(entries.map(async ([name, base]) => {
        try {
            await api(base, "/healthz");
            document.getElementById(`dot-${name}`).className = "indicator ok";
        } catch {
            document.getElementById(`dot-${name}`).className = "indicator error";
            allOk = false;
        }
    }));
    status.innerHTML = allOk
        ? '<div class="dot ok"></div><span>All services healthy</span>'
        : '<div class="dot error"></div><span>Some services are down</span>';
}

// ----- Events (simulated: poll Kafka via REST gateway) -----
async function refreshEvents() {
    // The Ballerina services don't expose a Kafka browser endpoint.
    // Show the latest outbox payload by hitting /healthz + sample call.
    const topic = $("#topic-select").value;
    $("#event-log").textContent =
        `Topic: ${topic}\n\n` +
        `Kafka event stream inspection is available via:\n` +
        `  • CLI: docker exec mjolnir-food-delivery-kafka-1 \\\n` +
        `        kafka-console-consumer --bootstrap-server kafka:9092 \\\n` +
        `        --topic ${topic} --from-beginning --max-messages 50\n` +
        `\n` +
        `All domain events are published through the outbox pattern, so\n` +
        `you can also verify via PostgreSQL:\n` +
        `  • docker exec mjolnir-food-delivery-postgres-1 psql -U postgres \\\n` +
        `        -c "SELECT topic, event_id, occurred_at FROM <db>_db.outbox_events\n` +
        `           WHERE published_at IS NULL ORDER BY id DESC LIMIT 20"`;
}
$("#refresh-events").addEventListener("click", refreshEvents);

// ----- Customers -----
async function refreshCustomers() {
    try {
        const list = await api(SERVICES.customer, "/customers");
        const tbody = $("#customers-table tbody");
        tbody.innerHTML = "";
        list.forEach(c => {
            tbody.innerHTML += `<tr>
                <td><code>${c.id.substring(0, 8)}…</code></td>
                <td>${c.email}</td>
                <td>${c.fullName}</td>
                <td>${c.phone || "-"}</td>
                <td>${c.createdAt.substring(0, 19)}</td>
            </tr>`;
        });
        // populate dropdowns for orders
        const sel = $("#order-customer-select");
        sel.innerHTML = list.map(c => `<option value="${c.id}">${c.fullName}</option>`).join("");
    } catch {}
}
$('#customer-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    const body = { email: fd.get('email'), fullName: fd.get('fullName') };
    if (fd.get('phone')) body.phone = fd.get('phone');
    const headers = {};
    const k = fd.get('idempotencyKey');
    if (k) headers['Idempotency-Key'] = k;
    try {
        await api(SERVICES.customer, '/customers', { method: 'POST', body: JSON.stringify(body), headers });
        toast('Customer created', 'success');
        e.target.reset();
        refreshCustomers();
    } catch {}
});

// ----- Restaurants & menus -----
async function refreshRestaurants() {
    try {
        const list = await api(SERVICES.restaurant, "/restaurants");
        const tbody = $("#restaurants-table tbody");
        tbody.innerHTML = "";
        list.forEach(r => {
            tbody.innerHTML += `<tr>
                <td><code>${r.id.substring(0, 8)}…</code></td>
                <td>${r.name}</td>
                <td>${r.cuisine || "-"}</td>
                <td><span class="state-pill state-${r.isOpen ? 'CONFIRMED' : 'CANCELLED'}">${r.isOpen ? 'OPEN' : 'CLOSED'}</span></td>
                <td>${r.opensAt.substring(0,5)} – ${r.closesAt.substring(0,5)}</td>
            </tr>`;
        });
        const sel = $("#restaurant-select");
        const sel2 = $("#order-restaurant-select");
        sel.innerHTML = list.map(r => `<option value="${r.id}">${r.name}</option>`).join("");
        sel2.innerHTML = list.map(r => `<option value="${r.id}">${r.name}</option>`).join("");
        // Populate menu table for first restaurant
        if (list.length > 0) refreshMenu(list[0].id);
    } catch {}
}
async function refreshMenu(restaurantId) {
    try {
        const items = await api(SERVICES.restaurant, `/restaurants/${restaurantId}/menu`);
        const tbody = $("#menu-table tbody");
        tbody.innerHTML = "";
        items.forEach(m => {
            tbody.innerHTML += `<tr>
                <td><code>${m.id.substring(0, 8)}…</code></td>
                <td>${m.name.substring(0, 20)}</td>
                <td>${m.name}</td>
                <td>${m.priceCents}</td>
                <td>${m.stockQty}</td>
            </tr>`;
        });
    } catch {}
}
$('#restaurant-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    const body = { name: fd.get('name') };
    if (fd.get('cuisine')) body.cuisine = fd.get('cuisine');
    if (fd.get('address')) body.address = fd.get('address');
    body.opensAt = fd.get('opensAt');
    body.closesAt = fd.get('closesAt');
    try {
        await api(SERVICES.restaurant, '/restaurants', { method: 'POST', body: JSON.stringify(body) });
        toast('Restaurant created', 'success');
        e.target.reset();
        refreshRestaurants();
    } catch {}
});
$('#menu-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    const body = {
        name: fd.get('name'),
        priceCents: parseInt(fd.get('priceCents')),
        initialStock: parseInt(fd.get('initialStock'))
    };
    if (fd.get('description')) body.description = fd.get('description');
    try {
        await api(SERVICES.restaurant, `/restaurants/${fd.get('restaurantId')}/menu`, { method: 'POST', body: JSON.stringify(body) });
        toast('Menu item added', 'success');
        refreshMenu(fd.get('restaurantId'));
    } catch {}
});

// ----- Drivers -----
async function refreshDrivers() {
    try {
        const list = await api(SERVICES.delivery, "/drivers");
        const tbody = $("#drivers-table tbody");
        tbody.innerHTML = "";
        list.forEach(d => {
            tbody.innerHTML += `<tr>
                <td><code>${d.id.substring(0, 8)}…</code></td>
                <td>${d.fullName}</td>
                <td>${d.vehicle || "-"}</td>
                <td><span class="state-pill state-${d.isAvailable ? 'ASSIGNED' : 'PICKED_UP'}">${d.isAvailable ? 'AVAILABLE' : 'BUSY'}</span></td>
            </tr>`;
        });
    } catch {}
}
$('#driver-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    const body = { fullName: fd.get('fullName') };
    if (fd.get('phone')) body.phone = fd.get('phone');
    if (fd.get('vehicle')) body.vehicle = fd.get('vehicle');
    try {
        await api(SERVICES.delivery, '/drivers', { method: 'POST', body: JSON.stringify(body) });
        toast('Driver registered', 'success');
        e.target.reset();
        refreshDrivers();
    } catch {}
});

// ----- Orders -----
async function refreshOrders() {
    try {
        // The order service doesn't have a list-all endpoint; we list from
        // multiple places (customers, restaurant) — for demo we use the
        // admin summary as a hint
        const tbody = $("#orders-table tbody");
        tbody.innerHTML = '<tr><td colspan="6" class="hint">Orders are visible from a given order ID via <code>GET /orders/{id}</code>. Place an order above to see its lifecycle in real-time.</td></tr>';
    } catch {}
}
$('#order-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    const body = {
        customerId: fd.get('customerId'),
        restaurantId: fd.get('restaurantId'),
        deliveryAddressId: fd.get('deliveryAddressId'),
        items: [{ menuItemId: fd.get('menuItemId'), qty: parseInt(fd.get('qty')) }]
    };
    const headers = {};
    const k = fd.get('idempotencyKey');
    if (k) headers['Idempotency-Key'] = k;
    try {
        const order = await api(SERVICES.order, '/orders', { method: 'POST', body: JSON.stringify(body), headers });
        toast(`Order created: ${order.data.id.substring(0,8)}…`, 'success');
        renderOrderRow(order.data);
        // simulate walk: confirm -> start preparing -> ready -> dispatch -> deliver
        await walkOrder(order.data.id);
    } catch {}
});

function renderOrderRow(o) {
    const tbody = $("#orders-table tbody");
    // Drop the placeholder row if present
    const placeholder = tbody.querySelector('td.hint');
    if (placeholder) tbody.innerHTML = "";
    tbody.innerHTML += `<tr id="order-${o.id}">
        <td><code>${o.id.substring(0, 8)}…</code></td>
        <td>${o.customerId.substring(0, 8)}…</td>
        <td>${o.restaurantId.substring(0, 8)}…</td>
        <td>${o.totalCents}¢</td>
        <td><span class="state-pill state-${o.state}">${o.state}</span></td>
        <td id="order-actions-${o.id}">${renderOrderActions(o)}</td>
    </tr>`;
}
function renderOrderActions(o) {
    const transitions = {
        CREATED: ['confirm', 'cancel'],
        CONFIRMED: ['startPreparing', 'cancel'],
        PREPARING: ['ready'],
        READY: ['dispatch'],
        OUT_FOR_DELIVERY: ['deliver']
    };
    const labels = {
        confirm: 'Confirm & Reserve',
        cancel: 'Cancel',
        startPreparing: 'Start preparing',
        ready: 'Mark ready',
        dispatch: 'Dispatch driver',
        deliver: 'Mark delivered'
    };
    const allowed = transitions[o.state] || [];
    return allowed.map(t => `<button class="action-btn" onclick="transitionOrder('${o.id}', '${t}')">${labels[t]}</button>`).join('');
}
window.transitionOrder = async (id, action) => {
    try {
        await api(SERVICES.order, `/orders/${id}/${action}`, { method: 'POST' });
        toast(`Order ${action}: OK`, 'success');
        const o = await api(SERVICES.order, `/orders/${id}`);
        document.getElementById(`order-${id}`)?.remove();
        renderOrderRow(o.data);
        if (action === 'confirm') await refreshPayments();
        if (action === 'dispatch') await refreshDeliveries();
        if (action === 'deliver') refreshReports();
    } catch {}
};
async function walkOrder(id) {
    // Demo: walk through the state machine for educational purposes.
    const steps = [
        ['confirm', 1000],
        ['startPreparing', 1000],
        ['ready', 1000],
        ['dispatch', 1000],
        ['deliver', 1000]
    ];
    for (const [action, delay] of steps) {
        await new Promise(r => setTimeout(r, delay));
        try {
            const o = await api(SERVICES.order, `/orders/${id}`);
            const allowedNext = ({
                CREATED: 'confirm',
                CONFIRMED: 'startPreparing',
                PREPARING: 'ready',
                READY: 'dispatch',
                OUT_FOR_DELIVERY: 'deliver'
            })[o.data.state];
            if (allowedNext !== action) continue;
            await transitionOrder(id, action);
        } catch (e) {
            console.log("walk step", action, "failed", e);
            break;
        }
    }
}

// ----- Payments -----
async function refreshPayments() {
    try {
        const list = await api(SERVICES.payment, "/payments");
        const tbody = $("#payments-table tbody");
        tbody.innerHTML = "";
        list.forEach(p => {
            tbody.innerHTML += `<tr>
                <td><code>${p.id.substring(0, 8)}…</code></td>
                <td>${p.orderId.substring(0, 8)}…</td>
                <td>${p.amountCents}¢</td>
                <td>${p.method}</td>
                <td><span class="state-pill state-${p.status}">${p.status}</span></td>
                <td>${p.failureReason || "-"}</td>
            </tr>`;
        });
    } catch {}
}

// ----- Deliveries -----
async function refreshDeliveries() {
    try {
        const orders = await api(SERVICES.order, "/orders"); // may 404; OK
    } catch {}
    // No list endpoint on delivery; rely on per-order lookup triggered by dispatch
    const tbody = $("#deliveries-table tbody");
    tbody.innerHTML = '<tr><td colspan="5" class="hint">Deliveries appear here after a driver is dispatched.</td></tr>';
}

// ----- Notifications -----
async function refreshNotifications() {
    try {
        const list = await api(SERVICES.notification, "/notifications");
        const tbody = $("#notifications-table tbody");
        tbody.innerHTML = "";
        list.forEach(n => {
            tbody.innerHTML += `<tr>
                <td><code>${n.id.substring(0, 8)}…</code></td>
                <td>${n.recipientId.substring(0, 8)}…</td>
                <td>${n.channel}</td>
                <td>${(n.body || '').substring(0, 60)}</td>
                <td><span class="state-pill state-${n.status}">${n.status}</span></td>
                <td>${(n.sentAt || '').substring(0, 19) || "-"}</td>
            </tr>`;
        });
    } catch {}
}
$('#notification-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    const body = { recipientId: fd.get('recipientId'),
        channel: fd.get('channel'), body: fd.get('body') };
    if (fd.get('subject')) body.subject = fd.get('subject');
    try {
        await api(SERVICES.notification, '/notifications', { method: 'POST', body: JSON.stringify(body) });
        toast('Notification sent', 'success');
        e.target.reset();
        refreshNotifications();
    } catch {}
});

// ----- Reports -----
async function refreshReports() {
    try {
        const summary = await api(SERVICES.admin, "/reports/summary");
        $("#summary").textContent = JSON.stringify(summary, null, 2);
    } catch {
        $("#summary").textContent = "Admin service unavailable";
    }
    try {
        const orderStats = await api(SERVICES.admin, "/reports/orders?maxRows=20");
        const ot = $("#order-stats tbody");
        ot.innerHTML = "";
        orderStats.forEach(o => {
            ot.innerHTML += `<tr>
                <td>${o.day}</td>
                <td><code>${o.restaurantId.substring(0, 8)}…</code></td>
                <td>${o.orderCount}</td>
                <td>${o.deliveredCount}</td>
                <td>${o.cancelledCount}</td>
                <td>${o.revenueCents}</td>
            </tr>`;
        });
    } catch {}
    try {
        const delStats = await api(SERVICES.admin, "/reports/deliveries?maxRows=20");
        const dt = $("#delivery-stats tbody");
        dt.innerHTML = "";
        delStats.forEach(d => {
            dt.innerHTML += `<tr>
                <td>${d.day}</td>
                <td><code>${d.driverId.substring(0, 8)}…</code></td>
                <td>${d.assignedCount}</td>
                <td>${d.deliveredCount}</td>
                <td>${d.avgDeliveryMinutes ? d.avgDeliveryMinutes.toFixed(1) : "-"}</td>
            </tr>`;
        });
    } catch {}
}
$("#refresh-reports").addEventListener("click", refreshReports);

// Initial load
checkHealth();
refreshCustomers();
refreshRestaurants();
refreshDrivers();
refreshNotifications();
refreshEvents();
setInterval(checkHealth, 15000);