// planner generates a query plan from an AST (abstract syntax tree) generated
// by the compiler. The query plan is a tree structure similar to relational
// algebra. The query plan is then converted to bytecode and fed to the vm
// (virtual machine) to be ran.
package planner
