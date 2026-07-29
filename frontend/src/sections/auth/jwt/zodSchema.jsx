import { z } from "zod";

export const userDataSchema = z
  .object({
    role: z.string().optional(),
    customRole: z.string().optional(),
    // Goals are entirely optional — a user may pick any, all, or none. Allow
    // undefined elements (checkboxes never toggled) so no "Required" error is
    // raised and Skip/Continue aren't blocked by validation.
    goals: z.array(z.boolean().optional()).optional(),
  })
  .refine((data) => data.role?.trim() || data.customRole?.trim(), {
    message: "Please select a role or enter your role",
    path: ["role"],
  });

export const organizationSchema = z.object({
  orgName: z.string().min(1, "Organization name is required"),
  members: z
    .array(
      z.object({
        // An empty string is the untyped "draft" row (stripped on submit);
        // anything else must be a valid email.
        email: z.union([z.literal(""), z.string().email("Invalid email format")]),
        name: z.string().optional(),
        organization_role: z.string().min(1, "Role is required"),
        disabled: z.boolean().optional(),
      }),
    )
    .superRefine((members, ctx) => {
      const seen = new Map();

      members.forEach((member, index) => {
        const email = member.email.trim().toLowerCase();

        if (seen.has(email)) {
          ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: "Duplicate email not allowed",
            path: [index, "email"],
          });
        } else {
          seen.set(email, index);
        }
      });
    }),
});
