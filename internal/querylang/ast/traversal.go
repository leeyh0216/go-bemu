package ast

import "fmt"

// Relations returns every relation occurrence in deterministic pre-order.
// DML and DDL targets are included, allowing semantic analysis to require one
// binding for every NodeKey before an engine visitor runs.
func Relations(statement Statement) ([]Relation, error) {
	if statement == nil {
		return nil, fmt.Errorf("statement is nil")
	}
	collector := &relationCollector{}
	if err := collector.statement(statement); err != nil {
		return nil, err
	}
	return append([]Relation(nil), collector.relations...), nil
}

type relationCollector struct {
	relations []Relation
}

func (collector *relationCollector) statement(statement Statement) error {
	switch value := statement.(type) {
	case *ScriptStatement:
		for _, child := range value.statements {
			if err := collector.statement(child); err != nil {
				return err
			}
		}
	case *DeclareStatement:
		return collector.expression(value.defaultValue)
	case *SetStatement:
		return collector.expression(value.value)
	case *SelectStatement:
		return collector.query(value.query)
	case *InsertStatement:
		if err := collector.relation(value.target); err != nil {
			return err
		}
		for _, row := range value.rows {
			for _, expression := range row {
				if err := collector.expression(expression); err != nil {
					return err
				}
			}
		}
		if value.query != nil {
			return collector.query(*value.query)
		}
	case *UpdateStatement:
		if err := collector.relation(value.target); err != nil {
			return err
		}
		for _, assignment := range value.assignments {
			if err := collector.expression(assignment.value); err != nil {
				return err
			}
		}
		if err := collector.relation(value.from); err != nil {
			return err
		}
		return collector.expression(value.where)
	case *DeleteStatement:
		if err := collector.relation(value.target); err != nil {
			return err
		}
		return collector.expression(value.where)
	case *MergeStatement:
		if err := collector.relation(value.target); err != nil {
			return err
		}
		if err := collector.relation(value.source); err != nil {
			return err
		}
		if err := collector.expression(value.condition); err != nil {
			return err
		}
		for _, when := range value.when {
			if err := collector.expression(when.condition); err != nil {
				return err
			}
			for _, expression := range when.action.values {
				if err := collector.expression(expression); err != nil {
					return err
				}
			}
			for _, assignment := range when.action.assignments {
				if err := collector.expression(assignment.value); err != nil {
					return err
				}
			}
		}
	case *CreateTableStatement:
		return collector.relation(value.target)
	case *DropTableStatement:
		return collector.relation(value.target)
	case *AlterTableStatement:
		return collector.relation(value.target)
	case *TruncateTableStatement:
		return collector.relation(value.target)
	default:
		return fmt.Errorf("unknown statement kind")
	}
	return nil
}

func (collector *relationCollector) query(query Query) error {
	for _, expression := range query.with {
		if err := collector.query(expression.query); err != nil {
			return err
		}
	}
	if err := collector.queryBody(query.body); err != nil {
		return err
	}
	for _, item := range query.orderBy {
		if err := collector.expression(item.expression); err != nil {
			return err
		}
	}
	return nil
}

func (collector *relationCollector) queryBody(body QueryBody) error {
	switch value := body.(type) {
	case *SelectQuery:
		for _, item := range value.items {
			if err := collector.expression(item.expression); err != nil {
				return err
			}
		}
		if err := collector.relation(value.from); err != nil {
			return err
		}
		if err := collector.expression(value.where); err != nil {
			return err
		}
		for _, expression := range value.groupBy {
			if err := collector.expression(expression); err != nil {
				return err
			}
		}
		if err := collector.expression(value.having); err != nil {
			return err
		}
		return collector.expression(value.qualify)
	case *SetOperationQuery:
		if err := collector.queryBody(value.left); err != nil {
			return err
		}
		return collector.queryBody(value.right)
	default:
		return fmt.Errorf("unknown query body kind")
	}
}

func (collector *relationCollector) relation(relation Relation) error {
	if relation == nil {
		return nil
	}
	collector.relations = append(collector.relations, relation)
	switch value := relation.(type) {
	case *TableRelation:
		return nil
	case *SubqueryRelation:
		return collector.query(value.query)
	case *JoinRelation:
		if err := collector.relation(value.left); err != nil {
			return err
		}
		if err := collector.relation(value.right); err != nil {
			return err
		}
		return collector.expression(value.condition.on)
	case *UnnestRelation:
		return collector.expression(value.expression)
	default:
		return fmt.Errorf("unknown relation kind")
	}
}

func (collector *relationCollector) expression(expression Expression) error {
	if expression == nil {
		return nil
	}
	switch value := expression.(type) {
	case *IdentifierExpression, *StarExpression, *NullLiteral, *BooleanLiteral,
		*IntegerLiteral, *FloatLiteral, *StringLiteral, *TemporalLiteral:
		return nil
	case *ArrayLiteral:
		for _, element := range value.elements {
			if err := collector.expression(element); err != nil {
				return err
			}
		}
	case *StructLiteral:
		for _, field := range value.fields {
			if err := collector.expression(field.value); err != nil {
				return err
			}
		}
	case *FunctionCall:
		for _, argument := range value.arguments {
			if err := collector.expression(argument); err != nil {
				return err
			}
		}
	case *UnaryExpression:
		return collector.expression(value.value)
	case *BinaryExpression:
		if err := collector.expression(value.left); err != nil {
			return err
		}
		return collector.expression(value.right)
	case *CastExpression:
		return collector.expression(value.value)
	case *InExpression:
		if err := collector.expression(value.value); err != nil {
			return err
		}
		for _, option := range value.options {
			if err := collector.expression(option); err != nil {
				return err
			}
		}
		if value.subquery != nil {
			return collector.query(*value.subquery)
		}
		return collector.expression(value.unnest)
	case *ParenthesizedExpression:
		return collector.expression(value.inner)
	case *SubqueryExpression:
		return collector.query(value.query)
	default:
		return fmt.Errorf("unknown expression kind")
	}
	return nil
}
