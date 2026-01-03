package planner

import (
	"github.com/chirst/cdb/vm"
)

func (u *updateNode) produce() {
	u.child.produce()
}

func (u *updateNode) consume() {
	// RowID
	u.plan.commands = append(u.plan.commands, &vm.RowIdCmd{
		P1: u.cursorId,
		P2: u.plan.freeRegister,
	})
	rowIdRegister := u.plan.freeRegister
	u.plan.freeRegister += 1

	// Reserve a contiguous block of free registers for the columns. This block
	// will be used in makeRecord.
	startRecordRegister := u.plan.freeRegister
	u.plan.freeRegister += len(u.updateExprs)
	recordRegisterCount := len(u.updateExprs)
	for i, e := range u.updateExprs {
		generateExpressionTo(u.plan, e, startRecordRegister+i, u.cursorId)
	}

	// Make the record for inserting
	u.plan.commands = append(u.plan.commands, &vm.MakeRecordCmd{
		P1: startRecordRegister,
		P2: recordRegisterCount,
		P3: u.plan.freeRegister,
	})
	recordRegister := u.plan.freeRegister
	u.plan.freeRegister += 1

	// Update by deleting then inserting
	u.plan.commands = append(u.plan.commands, &vm.DeleteCmd{
		P1: u.cursorId,
	})
	u.plan.commands = append(u.plan.commands, &vm.InsertCmd{
		P1: u.cursorId,
		P2: recordRegister,
		P3: rowIdRegister,
	})
}

func (f *filterNode) produce() {
	f.child.produce()
}

func (f *filterNode) consume() {
	jumpCommand := generatePredicate(f.plan, f.predicate, f.cursorId)
	f.parent.consume()
	jumpCommand.SetJumpAddress(len(f.plan.commands))
}

func (s *scanNode) produce() {
	s.consume()
}

func (s *scanNode) consume() {
	if s.isWriteCursor {
		s.plan.commands = append(
			s.plan.commands,
			&vm.OpenWriteCmd{P1: s.cursorId, P2: s.rootPageNumber},
		)
	} else {
		s.plan.commands = append(
			s.plan.commands,
			&vm.OpenReadCmd{P1: s.cursorId, P2: s.rootPageNumber},
		)
	}
	rewindCmd := &vm.RewindCmd{P1: s.cursorId}
	s.plan.commands = append(s.plan.commands, rewindCmd)
	loopBeginAddress := len(s.plan.commands)
	s.parent.consume()
	s.plan.commands = append(s.plan.commands, &vm.NextCmd{
		P1: s.cursorId,
		P2: loopBeginAddress,
	})
	rewindCmd.P2 = len(s.plan.commands)
}

func (p *projectNode) produce() {
	p.child.produce()
}

func (p *projectNode) consume() {
	startRegister := p.plan.freeRegister
	reservedRegisters := len(p.projections)
	p.plan.freeRegister += reservedRegisters
	for i, projection := range p.projections {
		generateExpressionTo(p.plan, projection.expr, startRegister+i, p.cursorId)
	}
	p.plan.commands = append(p.plan.commands, &vm.ResultRowCmd{
		P1: startRegister,
		P2: reservedRegisters,
	})
}

func (c *constantNode) produce() {
	c.consume()
}

func (c *constantNode) consume() {
	c.parent.consume()
}

func (c *countNode) produce() {
	c.consume()
}

func (c *countNode) consume() {
	c.plan.commands = append(
		c.plan.commands,
		&vm.OpenReadCmd{P1: c.cursorId, P2: c.rootPageNumber},
	)
	c.plan.commands = append(c.plan.commands, &vm.CountCmd{
		P1: c.cursorId,
		P2: c.plan.freeRegister,
	})
	countRegister := c.plan.freeRegister
	countResults := 1
	c.plan.freeRegister += 1
	c.plan.commands = append(c.plan.commands, &vm.ResultRowCmd{
		P1: countRegister,
		P2: countResults,
	})
}

func (c *createNode) produce() {
	c.consume()
}

func (c *createNode) consume() {
	if c.noop {
		return
	}
	c.plan.commands = append(
		c.plan.commands,
		&vm.OpenWriteCmd{P1: c.catalogCursorId, P2: c.catalogRootPageNumber},
	)
	c.plan.commands = append(c.plan.commands, &vm.CreateBTreeCmd{P2: 1})
	c.plan.commands = append(c.plan.commands, &vm.NewRowIdCmd{P1: c.catalogCursorId, P2: 2})
	c.plan.commands = append(c.plan.commands, &vm.StringCmd{P1: 3, P4: c.objectType})
	c.plan.commands = append(c.plan.commands, &vm.StringCmd{P1: 4, P4: c.objectName})
	c.plan.commands = append(c.plan.commands, &vm.StringCmd{P1: 5, P4: c.tableName})
	c.plan.commands = append(c.plan.commands, &vm.CopyCmd{P1: 1, P2: 6})
	c.plan.commands = append(c.plan.commands, &vm.StringCmd{P1: 7, P4: string(c.schema)})
	c.plan.commands = append(c.plan.commands, &vm.MakeRecordCmd{P1: 3, P2: 5, P3: 8})
	c.plan.commands = append(c.plan.commands, &vm.InsertCmd{P1: c.catalogCursorId, P2: 8, P3: 2})
	c.plan.commands = append(c.plan.commands, &vm.ParseSchemaCmd{})
}

func (n *insertNode) produce() {
	n.consume()
}

func (n *insertNode) consume() {
	n.plan.commands = append(
		n.plan.commands,
		&vm.OpenWriteCmd{P1: n.cursorId, P2: n.rootPageNumber},
	)
	for valuesIdx := range len(n.colValues) {
		// Setup rowid and it's uniqueness/type checks
		pkRegister := n.plan.freeRegister
		n.plan.freeRegister += 1
		if n.autoPk {
			n.plan.commands = append(n.plan.commands, &vm.NewRowIdCmd{
				P1: n.cursorId,
				P2: pkRegister,
			})
		} else {
			generateExpressionTo(n.plan, n.pkValues[valuesIdx], pkRegister, n.cursorId)
			n.plan.commands = append(n.plan.commands, &vm.MustBeIntCmd{P1: pkRegister})
			nec := &vm.NotExistsCmd{
				P1: n.cursorId,
				P3: pkRegister,
			}
			n.plan.commands = append(n.plan.commands, nec)
			n.plan.commands = append(n.plan.commands, &vm.HaltCmd{
				P1: 1,
				P4: pkConstraint,
			})
			nec.P2 = len(n.plan.commands)
		}

		// Reserve registers and make values segment for MakeRecord
		startRegister := n.plan.freeRegister
		reservedRegisters := len(n.colValues[valuesIdx])
		n.plan.freeRegister += reservedRegisters
		for vi := range n.colValues[valuesIdx] {
			generateExpressionTo(
				n.plan,
				n.colValues[valuesIdx][vi],
				startRegister+vi,
				n.cursorId,
			)
		}

		// Insert
		n.plan.commands = append(n.plan.commands, &vm.MakeRecordCmd{
			P1: startRegister,
			P2: reservedRegisters,
			P3: n.plan.freeRegister,
		})
		recordRegister := n.plan.freeRegister
		n.plan.freeRegister += 1
		n.plan.commands = append(n.plan.commands, &vm.InsertCmd{
			P1: n.cursorId,
			P2: recordRegister,
			P3: pkRegister,
		})
	}
}

func (n *joinNode) produce() {}

func (n *joinNode) consume() {}
