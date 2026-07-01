// Lives in lib, NOT in the `_db` route file: Start code-splits route files, so
// a context exported from one is evaluated twice (split component module vs.
// direct import) and the provider and consumers end up holding two different
// contexts — the SSR pass then renders the "outside the layout" error.

import * as React from "react"
import type { TodosCollection } from "./todos.ts"

export const TodosContext = React.createContext<TodosCollection | null>(null)

export function useTodos(): TodosCollection {
  const todos = React.useContext(TodosContext)
  if (!todos) throw new Error("useTodos must be used under the /_db layout")
  return todos
}
