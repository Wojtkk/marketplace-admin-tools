import logging
from uuid import UUID

import requests
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel

from admin_api.audit import log_action, get_audit_trail
from admin_api.models import AdminUser

logger = logging.getLogger(__name__)

app = FastAPI(title="Marketplace Admin Panel", version="1.0.0")

GATEWAY_URL = "http://gateway:8000"
PAYMENT_SERVICE_URL = "http://payment-service:8003"
CATALOG_SERVICE_URL = "http://catalog-service:8001"
ANALYTICS_SERVICE_URL = "http://analytics-service:8010"


class RefundRequest(BaseModel):
    reason: str
    amount: float | None = None


class DisableProductRequest(BaseModel):
    reason: str


def _gateway_get(path: str, params: dict | None = None) -> dict:
    try:
        response = requests.get(f"{GATEWAY_URL}{path}", params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        logger.error("Gateway request failed for %s: %s", path, exc)
        raise HTTPException(status_code=502, detail=f"Gateway unavailable: {exc}")


@app.get("/admin/orders")
def list_orders(
    status: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict:
    params = {"page": page, "page_size": page_size}
    if status:
        params["status"] = status
    data = _gateway_get("/api/orders", params=params)
    return {"orders": data.get("orders", []), "total": data.get("total", 0)}


@app.get("/admin/orders/{order_id}")
def get_order(order_id: UUID) -> dict:
    data = _gateway_get(f"/api/orders/{order_id}")
    return {"order": data}


@app.post("/admin/orders/{order_id}/refund")
def refund_order(order_id: UUID, body: RefundRequest, admin_id: UUID | None = None) -> dict:
    try:
        payload = {
            "order_id": str(order_id),
            "reason": body.reason,
        }
        if body.amount is not None:
            payload["amount"] = body.amount

        response = requests.post(
            f"{PAYMENT_SERVICE_URL}/payments/refund",
            json=payload,
            timeout=15,
        )
        response.raise_for_status()
        result = response.json()
    except requests.RequestException as exc:
        logger.error("Refund request failed for order %s: %s", order_id, exc)
        raise HTTPException(status_code=502, detail=f"Payment service unavailable: {exc}")

    if admin_id:
        log_action(
            admin_id=admin_id,
            action="refund_order",
            target_type="order",
            target_id=str(order_id),
            details={"reason": body.reason, "amount": body.amount},
        )

    return {"refund": result}


@app.get("/admin/products")
def list_products(
    category: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict:
    params = {"page": page, "page_size": page_size}
    if category:
        params["category"] = category
    data = _gateway_get("/api/products", params=params)
    return {"products": data.get("products", []), "total": data.get("total", 0)}


@app.post("/admin/products/{product_id}/disable")
def disable_product(
    product_id: UUID,
    body: DisableProductRequest,
    admin_id: UUID | None = None,
) -> dict:
    try:
        response = requests.put(
            f"{CATALOG_SERVICE_URL}/products/{product_id}/stock",
            json={"stock_quantity": 0, "is_active": False},
            timeout=10,
        )
        response.raise_for_status()
        result = response.json()
    except requests.RequestException as exc:
        logger.error("Disable product failed for %s: %s", product_id, exc)
        raise HTTPException(status_code=502, detail=f"Catalog service unavailable: {exc}")

    if admin_id:
        log_action(
            admin_id=admin_id,
            action="disable_product",
            target_type="product",
            target_id=str(product_id),
            details={"reason": body.reason},
        )

    return {"product": result, "disabled": True}


@app.get("/admin/users/{user_id}/orders")
def get_user_orders(user_id: UUID, page: int = 1, page_size: int = 20) -> dict:
    data = _gateway_get(
        f"/api/orders",
        params={"user_id": str(user_id), "page": page, "page_size": page_size},
    )
    return {"user_id": str(user_id), "orders": data.get("orders", [])}


@app.get("/admin/dashboard")
def get_dashboard() -> dict:
    try:
        revenue_resp = requests.get(
            f"{ANALYTICS_SERVICE_URL}/reports/revenue",
            timeout=10,
        )
        revenue_resp.raise_for_status()
        revenue_data = revenue_resp.json()

        metrics_resp = requests.get(
            f"{ANALYTICS_SERVICE_URL}/metrics/orders",
            timeout=10,
        )
        metrics_resp.raise_for_status()
        metrics_data = metrics_resp.json()

        top_products_resp = requests.get(
            f"{ANALYTICS_SERVICE_URL}/reports/top-products",
            params={"limit": 5},
            timeout=10,
        )
        top_products_resp.raise_for_status()
        top_products_data = top_products_resp.json()
    except requests.RequestException as exc:
        logger.error("Analytics service unavailable: %s", exc)
        raise HTTPException(status_code=502, detail=f"Analytics service unavailable: {exc}")

    return {
        "revenue": revenue_data,
        "order_metrics": metrics_data,
        "top_products": top_products_data.get("products", []),
    }


@app.get("/admin/audit/{target_type}/{target_id}")
def get_audit(target_type: str, target_id: str) -> dict:
    trail = get_audit_trail(target_type=target_type, target_id=target_id)
    return {"audit_trail": trail}
