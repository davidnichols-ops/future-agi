import PropTypes from "prop-types";
import { Box, Button, Typography } from "@mui/material";
import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { Helmet } from "react-helmet-async";
import UserHeaders from "./UserHeaders";
import GridTable from "./GridTable";
import { getUserQueryOptions } from "./getUserQueryOptions";
import { useQueryClient } from "@tanstack/react-query";
import { format } from "date-fns";
import {
  ProcessingStatusCell,
  ActionRender,
  OrgRoleCell,
  WorkspaceRoleCell,
  WorkspaceChipsCell,
  useCanSendInvite,
} from "./CellRender";
import AllActionForm from "./AllActionForm";
import WorkspaceDetailPanel from "./WorkspaceDetailPanel";
import { useAuthContext } from "src/auth/hooks";
import { useWorkspace } from "src/contexts/WorkspaceContext";
import { useLocation, useNavigate, useParams } from "react-router";
import BackButton from "src/sections/develop-detail/Common/BackButton";
import { LEVELS } from "./constant";
import { useUserManagementStore } from "./UserManagementStore";
import { endpoints } from "src/utils/axios";
import { gridSortModelToMemberListSort } from "./memberListGridQuery";
import OssInviteDialog from "./oss-invite/OssInviteDialog";
import { isProtoSession, getInvites, inviteToRow } from "./oss-invite/ossInviteState";
import { InviteLinkCell, InviteActionCell } from "./oss-invite/ProtoInviteCells";

const UserManagementV2 = ({ workspaceScope = false }) => {
  const { workspaceId: workspaceIdParam } = useParams();
  let workspaceId = workspaceIdParam;

  const location = useLocation();
  const navigate = useNavigate();
  const { role, user, orgLevel, effectiveLevel } = useAuthContext();
  const { currentWorkspaceId } = useWorkspace();
  const queryClient = useQueryClient();
  const gridApiRef = useRef(null);
  const overlayTimeoutRef = useRef(null);
  const [inviteUser, setInviteUser] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedStatus, setSelectedStatus] = useState("");
  const [selectedRole, setSelectedRole] = useState("");
  const { setUsersList } = useUserManagementStore();
  workspaceId = workspaceId ?? currentWorkspaceId ?? user?.default_workspace_id;

  // Use integer levels when available, fall back to string role check
  const canManageUsers =
    (orgLevel != null && orgLevel >= LEVELS.ADMIN) ||
    (effectiveLevel != null && effectiveLevel >= LEVELS.ADMIN) ||
    role === "Owner" ||
    role === "Admin" ||
    role === "workspace_admin";

  const { allowed: canSendInvite } = useCanSendInvite(orgLevel, effectiveLevel);

  // OSS prototype: drive invites through the local link/SMTP flow (the real
  // RBAC endpoints 401 under the proto session). Org-level Members page only.
  const protoInvite = isProtoSession() && !workspaceScope;
  const refreshGrid = useCallback(() => {
    gridApiRef.current?.api?.refreshServerSide({ purge: true });
  }, []);

  const columnDefs = useMemo(
    () => [
      {
        headerName: "User name",
        field: "name",
        flex: 1,
        // Show expand chevron for master/detail rows in org-level view
        ...(!workspaceScope ? { cellRenderer: "agGroupCellRenderer" } : {}),
      },
      {
        headerName: "Organisation Role",
        field: "org_role",
        colId: "org_level",
        flex: 1,
        cellRenderer: OrgRoleCell,
      },
      // Org-level view: show workspace chips; workspace-scoped view: show single role
      ...(workspaceScope
        ? [
            {
              headerName: "Workspace Role",
              field: "ws_role",
              colId: "ws_level",
              flex: 1,
              cellRenderer: WorkspaceRoleCell,
            },
          ]
        : [
            {
              headerName: "Workspaces",
              field: "workspaces",
              flex: 1.5,
              cellRenderer: WorkspaceChipsCell,
              sortable: false,
            },
          ]),
      {
        headerName: "Email",
        field: "email",
        flex: 1,
      },
      {
        headerName: "Status",
        field: "status",
        flex: 1,
        cellRenderer: ProcessingStatusCell,
      },
      {
        headerName: "Start date",
        field: "created_at",
        flex: 1,
        valueFormatter: (params) =>
          params?.value ? format(new Date(params?.value), "dd/MM/yyyy") : "",
      },
      // OSS proto: show the invite link inline (pending invites live in this
      // same table) with a copy button + delivery indicator.
      ...(protoInvite
        ? [
            {
              headerName: "Invite link",
              field: "invite_link",
              flex: 1.8,
              sortable: false,
              cellRenderer: InviteLinkCell,
            },
            {
              headerName: " ",
              field: "action",
              width: 56,
              sortable: false,
              cellRenderer: InviteActionCell,
              cellRendererParams: { onRefresh: refreshGrid },
            },
          ]
        : canManageUsers
          ? [
              {
                headerName: " ",
                field: "action",
                width: 50,
                cellRenderer: ActionRender,
                cellRendererParams: { workspaceScope, workspaceId },
                sortable: false,
              },
            ]
          : []),
    ],
    [canManageUsers, workspaceScope, protoInvite, refreshGrid, workspaceId],
  );

  // When workspaceScope is true, use workspace-specific member endpoint
  const wsEndpoint =
    workspaceScope && workspaceId
      ? endpoints.rbac.workspaceMemberList(workspaceId)
      : undefined;

  const getDataSource = (
    queryClient,
    overlayTimeoutRef,
    searchQuery,
    selectedStatus,
    selectedRole,
    workspaceId,
  ) => {
    return {
      getRows: async (params) => {
        const { request } = params;
        const pageNumber = Math.floor(request.startRow / 20);
        const sort = gridSortModelToMemberListSort(request?.sortModel, {
          workspaceScope,
        });
        const search = searchQuery || "";

        if (overlayTimeoutRef.current) {
          clearTimeout(overlayTimeoutRef.current);
          overlayTimeoutRef.current = null;
        }

        // Prototype session: the members API 401s (no real backend account),
        // which leaves the grid full of "ERR" cells and a stuck loading
        // overlay. Serve the current owner as a single real row instead — the
        // realistic state right after a fresh OSS setup (teammates are invited
        // later from inside the product).
        const isProtoSession =
          import.meta.env.VITE_PROTOTYPE_AUTH_BYPASS === "true" &&
          localStorage.getItem("oss_proto_session") === "1";
        if (isProtoSession) {
          const ownerRow = {
            id: "oss-proto-user",
            name: user?.name || "You",
            org_role: "Owner",
            org_level: LEVELS.OWNER,
            ws_role: "Owner",
            email: user?.email || "you@futureagi.com",
            status: "Active",
            created_at: new Date().toISOString(),
            workspaces: [
              {
                workspace_id: "default",
                workspace_name: "Default",
                ws_role: "Owner",
                ws_level: LEVELS.OWNER,
              },
            ],
          };
          // Pending invites are shown inline in this same table.
          const inviteRows = getInvites().map(inviteToRow);
          const q = search.trim().toLowerCase();
          const rows = [ownerRow, ...inviteRows].filter(
            (r) => !q || `${r.name} ${r.email}`.toLowerCase().includes(q),
          );
          setUsersList(rows);
          params.api.setGridOption("context", { totalRowCount: rows.length });
          params.success({ rowData: rows, rowCount: rows.length });
          return;
        }

        try {
          const queryOptions = getUserQueryOptions(
            {
              pageNumber,
              sort,
              search: search,
              filterStatus: selectedStatus ? [selectedStatus] : [],
              filterRole: selectedRole ? [selectedRole] : [],
              workspaceId,
              endpoint: wsEndpoint,
            },
            { staleTime: 5000 },
          );
          const data = await queryClient.fetchQuery({ ...queryOptions });
          const responseData = data?.data?.result || data?.data;
          const rows = responseData?.results || [];
          const totalRows = responseData?.total || rows.length;
          setUsersList(rows);
          params.api.setGridOption("context", {
            totalRowCount: totalRows,
          });

          params.success({
            rowData: rows,
            rowCount: totalRows,
          });
        } catch (e) {
          params.fail();
          overlayTimeoutRef.current = setTimeout(() => {
            params.api.showLoadingOverlay();
          }, 100);
        }
      },
    };
  };

  useEffect(() => {
    if (gridApiRef?.current?.api) {
      gridApiRef?.current?.api?.collapseAll();

      const dataSource = getDataSource(
        queryClient,
        overlayTimeoutRef,
        searchQuery,
        selectedStatus,
        selectedRole,
        workspaceId,
      );
      gridApiRef.current?.api?.setGridOption(
        "serverSideDatasource",
        dataSource,
      );
      // Optionally refresh the data
      gridApiRef?.current?.api?.refreshServerSide({ purge: true });
    }
  }, [searchQuery, selectedStatus, selectedRole, workspaceId, queryClient]);

  const onGridReady = useCallback(
    (params) => {
      const dataSource = getDataSource(
        queryClient,
        overlayTimeoutRef,
        searchQuery,
        selectedStatus,
        selectedRole,
        workspaceId,
      );
      params.api.setGridOption("serverSideDatasource", dataSource);
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [queryClient, searchQuery, selectedStatus, selectedRole, workspaceId],
  );
  const pageTitle = useMemo(() => {
    // Only show workspace name when navigated via /workspace/:workspaceId
    if (workspaceIdParam && location?.state?.workspaceName) {
      return location.state.workspaceName;
    }
    return "Members";
  }, [workspaceIdParam, location]);

  return (
    <>
      <Helmet>
        <title>
          {workspaceScope ? "Workspace Members" : "User Management"}
        </title>
      </Helmet>
      <Box
        sx={{
          paddingX: "2px",
          ...(protoInvite && {
            display: "flex",
            flexDirection: "column",
            height: "calc(100vh - 24px)",
          }),
        }}
      >
        {workspaceId && !workspaceScope && (
          <Box mb={2} display="flex" gap={2}>
            <BackButton onBack={() => navigate(-1)} />
          </Box>
        )}
        <Box>
          <Typography
            sx={{
              typography: "m2",
              fontWeight: "fontWeightSemiBold",
              color: "text.primary",
            }}
          >
            {pageTitle}
          </Typography>
          <Typography
            sx={{
              typography: "s1",
              fontWeight: "fontWeightRegular",
              color: "text.primary",
              marginTop: (theme) => theme.spacing(0.5),
            }}
          >
            {workspaceScope
              ? "Manage workspace members and their roles"
              : "Manage who has access to workspace"}
          </Typography>
        </Box>
        <Box
          sx={{
            paddingX: 0,
            paddingY: 2,
            display: "flex",
            gap: 2,
            width: "100%",
            justifyContent: "space-between",
          }}
        >
          <UserHeaders
            searchQuery={searchQuery}
            setSearchQuery={setSearchQuery}
            selectedStatus={selectedStatus}
            setSelectedStatus={setSelectedStatus}
            selectedRole={selectedRole}
            setSelectedRole={setSelectedRole}
          />
          {(canSendInvite || canManageUsers) && (
            <Button
              variant="contained"
              color="primary"
              onClick={() => setInviteUser(true)}
            >
              Invite User
            </Button>
          )}
        </Box>
        {protoInvite ? (
          <OssInviteDialog
            open={inviteUser}
            onClose={() => setInviteUser(false)}
            onInvited={refreshGrid}
          />
        ) : (
          <AllActionForm
            openActionForm={inviteUser ? { action: "invite-user" } : null}
            onClose={() => setInviteUser(false)}
            gridApi={gridApiRef?.current?.api}
            workspaceId={workspaceId}
          />
        )}
        {/* table data */}
        <Box
          sx={
            protoInvite
              ? { flex: 1, minHeight: 240 }
              : {
                  height: `calc(100vh - ${workspaceId && !workspaceScope ? 210 : 160}px)`,
                }
          }
        >
          <GridTable
            // @ts-ignore
            onGridReady={onGridReady}
            ref={gridApiRef}
            columnDefs={columnDefs}
            otherGridOption={
              workspaceScope
                ? {}
                : {
                    masterDetail: true,
                    detailCellRenderer: WorkspaceDetailPanel,
                    detailRowAutoHeight: true,
                    keepDetailRows: true,
                    isRowMaster: (data) =>
                      data?.workspaces?.length > 0 && data?.status === "Active",
                  }
            }
          />
        </Box>
      </Box>
    </>
  );
};

UserManagementV2.propTypes = {
  workspaceScope: PropTypes.bool,
};

export default UserManagementV2;
