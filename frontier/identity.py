from __future__ import annotations

from dataclasses import dataclass

from frontier.errors import InstallError

IDENTITY_INCOMPLETE_CODE = "FRONTIER_PROFILE_IDENTITY_INCOMPLETE"
IDENTITY_MISMATCH_CODE = "FRONTIER_PROFILE_IDENTITY_MISMATCH"
ARTIFACT_PROFILE_MISMATCH_CODE = "ARTIFACT_PROFILE_MISMATCH"


def normalize_api_origin(url: str | None) -> str:
    return (url or "").strip().rstrip("/")


def canonical_id(value: str | None) -> str:
    return (value or "").strip()


def has_canonical_ids(organization_id: str | None, project_id: str | None) -> bool:
    return bool(canonical_id(organization_id) and canonical_id(project_id))


@dataclass(frozen=True)
class TenantIdentity:
    api_origin: str = ""
    organization_id: str = ""
    project_id: str = ""
    organization_name: str = ""
    project_name: str = ""

    @property
    def complete(self) -> bool:
        return has_canonical_ids(self.organization_id, self.project_id)


def tenant_identity(
    *,
    api_origin: str | None = "",
    organization_id: str | None = "",
    project_id: str | None = "",
    organization_name: str | None = "",
    project_name: str | None = "",
) -> TenantIdentity:
    return TenantIdentity(
        api_origin=normalize_api_origin(api_origin),
        organization_id=canonical_id(organization_id),
        project_id=canonical_id(project_id),
        organization_name=(organization_name or "").strip(),
        project_name=(project_name or "").strip(),
    )


def identities_equal(
    left: TenantIdentity,
    right: TenantIdentity,
    *,
    include_origin: bool = True,
) -> bool:
    if not left.complete or not right.complete:
        return False
    if include_origin:
        left_origin = normalize_api_origin(left.api_origin)
        right_origin = normalize_api_origin(right.api_origin)
        if left_origin and right_origin and left_origin != right_origin:
            return False
    return canonical_id(left.organization_id) == canonical_id(right.organization_id) and canonical_id(
        left.project_id
    ) == canonical_id(right.project_id)


def identity_mismatches(
    left: TenantIdentity,
    right: TenantIdentity,
    *,
    include_origin: bool = True,
) -> list[str]:
    mismatches: list[str] = []
    if include_origin:
        left_origin = normalize_api_origin(left.api_origin)
        right_origin = normalize_api_origin(right.api_origin)
        if left_origin and right_origin and left_origin != right_origin:
            mismatches.append(f"API origin ({left_origin} vs {right_origin})")
    if left.complete and right.complete:
        if canonical_id(left.organization_id) != canonical_id(right.organization_id):
            mismatches.append(
                "organization "
                f"({format_organization_identity(left.organization_name, left.organization_id)} vs "
                f"{format_organization_identity(right.organization_name, right.organization_id)})"
            )
        if canonical_id(left.project_id) != canonical_id(right.project_id):
            mismatches.append(
                "project "
                f"({format_project_identity(left.project_name, left.project_id)} vs "
                f"{format_project_identity(right.project_name, right.project_id)})"
            )
    elif left.complete or right.complete:
        mismatches.append("immutable organization/project IDs")
    return mismatches


def format_organization_identity(name: str | None, organization_id: str | None) -> str:
    label = (name or "").strip() or "(unknown)"
    ident = canonical_id(organization_id)
    return f"{label} ({ident})" if ident else label


def format_project_identity(name: str | None, project_id: str | None) -> str:
    label = (name or "").strip() or "(unknown)"
    ident = canonical_id(project_id)
    return f"{label} ({ident})" if ident else label


def identity_incomplete_error(*, next_action: str | None = None) -> InstallError:
    return InstallError(
        IDENTITY_INCOMPLETE_CODE,
        "The Frontier server did not return immutable organization and project IDs.",
        cause="Named profiles require a compatible SaaS version. Legacy login remains available.",
        next_action=next_action
        or "Use `frontier login --api-key` without --profile, or upgrade Frontier SaaS.",
        docs_path="/docs/authenticate",
    )


def identity_mismatch_error(
    *,
    explanation: str,
    cause: str,
    next_action: str,
) -> InstallError:
    return InstallError(
        IDENTITY_MISMATCH_CODE,
        explanation,
        cause=cause,
        next_action=next_action,
        docs_path="/docs/authenticate",
    )
