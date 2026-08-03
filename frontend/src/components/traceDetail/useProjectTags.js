import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { enqueueSnackbar } from "notistack";
import axios from "src/utils/axios";
import { apiPath } from "src/api/contracts/api-surface";

/** The project's reusable tags — `[{ name, color }]`. */
export function useProjectTags(projectId, enabled = true) {
  return useQuery({
    queryKey: ["project-tag-definitions", projectId],
    queryFn: async () => {
      const { data } = await axios.get(
        apiPath("/tracer/project/{id}/tag-definitions/", { id: projectId }),
      );
      return data?.result?.tags || [];
    },
    enabled: Boolean(enabled && projectId),
    staleTime: 30_000,
  });
}

/**
 * Delete a tag project-wide — removed from every trace and span that carries
 * it, not just the row in view. Confirm before calling. `mutate({ name })`.
 */
export function useDeleteProjectTag(projectId, { onSuccess } = {}) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ name }) =>
      axios.delete(
        apiPath("/tracer/project/{id}/tag-definitions/", { id: projectId }),
        {
          params: { name },
        },
      ),
    onSuccess: (res, variables) => {
      queryClient.invalidateQueries({
        queryKey: ["project-tag-definitions", projectId],
      });
      queryClient.invalidateQueries({ queryKey: ["trace-detail"] });
      onSuccess?.(res, variables);
    },
    onError: () => {
      enqueueSnackbar("Failed to delete tag", { variant: "error" });
    },
  });
}

/**
 * Rename/recolour a tag project-wide. Must go through the tag, not the row —
 * re-posting the row's list under a new name forks a second tag.
 * `mutate({ name, newName, color })`; one of newName/color required.
 */
export function useEditProjectTag(projectId, { onSuccess } = {}) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ name, newName, color }) =>
      axios.patch(
        apiPath("/tracer/project/{id}/tag-definitions/", { id: projectId }),
        {
          name,
          ...(newName ? { new_name: newName } : {}),
          ...(color ? { color } : {}),
        },
      ),
    onSuccess: (res, variables) => {
      queryClient.invalidateQueries({
        queryKey: ["project-tag-definitions", projectId],
      });
      queryClient.invalidateQueries({ queryKey: ["trace-detail"] });
      onSuccess?.(res, variables);
    },
    onError: () => {
      enqueueSnackbar("Failed to update tag", { variant: "error" });
    },
  });
}
