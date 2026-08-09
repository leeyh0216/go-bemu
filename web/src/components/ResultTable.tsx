import { Box, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Typography } from "@mui/material";
import type { QueryCell, SchemaField } from "../domain/models";
import { EmptyView } from "./StateViews";

const displayValue = (value: QueryCell) => {
  if (value === null) return <Typography component="span" variant="body2" color="text.secondary">NULL</Typography>;
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
};

export function ResultTable({ schema, rows }: { schema: SchemaField[]; rows: QueryCell[][] }) {
  if (rows.length === 0) return <EmptyView title="No rows" />;
  return (
    <TableContainer sx={{ maxHeight: "100%", borderTop: 1, borderColor: "divider" }}>
      <Table stickyHeader size="small" aria-label="Query results">
        <TableHead>
          <TableRow>
            <TableCell sx={{ width: 52, color: "text.secondary" }}>#</TableCell>
            {schema.map((field) => (
              <TableCell key={field.name}>
                <Typography variant="body2" sx={{ fontWeight: 600 }}>{field.name}</Typography>
                <Typography variant="caption" color="text.secondary">{field.type}</Typography>
              </TableCell>
            ))}
          </TableRow>
        </TableHead>
        <TableBody>
          {rows.map((row, rowIndex) => (
            <TableRow hover key={rowIndex}>
              <TableCell sx={{ color: "text.secondary" }}>{rowIndex + 1}</TableCell>
              {schema.map((field, columnIndex) => (
                <TableCell key={`${field.name}-${columnIndex}`} sx={{ maxWidth: 420, overflow: "hidden", textOverflow: "ellipsis" }}>
                  <Box component="span" sx={{ fontFamily: typeof row[columnIndex] === "number" ? "monospace" : "inherit" }}>
                    {displayValue(row[columnIndex] ?? null)}
                  </Box>
                </TableCell>
              ))}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
}
