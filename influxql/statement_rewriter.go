package influxql

import (
	"errors"
	"regexp"
)

// RewriteStatement rewrites stmt into a new statement, if applicable.
func RewriteStatement(stmt Statement) (Statement, error) {
	switch stmt := stmt.(type) {
	case *ShowFieldKeysStatement:
		return rewriteShowFieldKeysStatement(stmt)
	case *ShowFieldKeyCardinalityStatement:
		return rewriteShowFieldKeyCardinalityStatement(stmt)
	case *ShowMeasurementsStatement:
		return rewriteShowMeasurementsStatement(stmt)
	case *ShowMeasurementCardinalityStatement:
		return rewriteShowMeasurementCardinalityStatement(stmt)
	case *ShowSeriesStatement:
		return rewriteShowSeriesStatement(stmt)
	case *ShowSeriesCardinalityStatement:
		return rewriteShowSeriesCardinalityStatement(stmt)
	case *ShowTagKeysStatement:
		return rewriteShowTagKeysStatement(stmt)
	case *ShowTagKeyCardinalityStatement:
		return rewriteShowTagKeyCardinalityStatement(stmt)
	case *ShowTagValuesStatement:
		return rewriteShowTagValuesStatement(stmt)
	case *ShowTagValuesCardinalityStatement:
		return rewriteShowTagValuesCardinalityStatement(stmt)
	default:
		return stmt, nil
	}
}

func rewriteShowFieldKeysStatement(stmt *ShowFieldKeysStatement) (Statement, error) {
	return &SelectStatement{
		Fields: Fields([]*Field{
			{Expr: &VarRef{Val: "fieldKey"}},
			{Expr: &VarRef{Val: "fieldType"}},
		}),
		Sources:    rewriteSources(stmt.Sources, "_fieldKeys", stmt.Database),
		Condition:  rewriteSourcesCondition(stmt.Sources, nil),
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		SortFields: stmt.SortFields,
		OmitTime:   true,
		Dedupe:     true,
		IsRawQuery: true,
	}, nil
}

func rewriteShowFieldKeyCardinalityStatement(stmt *ShowFieldKeyCardinalityStatement) (Statement, error) {
	// Check for time in WHERE clause (not supported).
	if HasTimeExpr(stmt.Condition) {
		return nil, errors.New("SHOW FIELD KEY CARDINALITY doesn't support time in WHERE clause")
	}

	// Use all field keys, if zero.
	if len(stmt.Sources) == 0 {
		stmt.Sources = Sources{
			&Measurement{Regex: &RegexLiteral{Val: regexp.MustCompile(`.+`)}},
		}
	}

	return &SelectStatement{
		Fields: []*Field{
			{
				Expr: &Call{
					Name: "count",
					Args: []Expr{
						&Call{
							Name: "distinct",
							Args: []Expr{&VarRef{Val: "_fieldKey"}},
						},
					},
				},
				Alias: "count",
			},
		},
		Sources:    rewriteSources2(stmt.Sources, stmt.Database),
		Condition:  stmt.Condition,
		Dimensions: stmt.Dimensions,
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		OmitTime:   true,
	}, nil
}

func rewriteShowMeasurementsStatement(stmt *ShowMeasurementsStatement) (Statement, error) {
	var sources Sources
	if stmt.Source != nil {
		sources = Sources{stmt.Source}
	}

	// Currently time based SHOW MEASUREMENT queries can't be supported because
	// it's not possible to appropriate set operations such as a negated regex
	// using the query engine.
	if HasTimeExpr(stmt.Condition) {
		return nil, errors.New("SHOW MEASUREMENTS doesn't support time in WHERE clause")
	}

	// rewrite condition to push a source measurement into a "_name" tag.
	stmt.Condition = rewriteSourcesCondition(sources, stmt.Condition)
	return stmt, nil
}

func rewriteShowMeasurementCardinalityStatement(stmt *ShowMeasurementCardinalityStatement) (Statement, error) {
	// TODO(edd): currently we only support cardinality estimation for certain
	// types of query. As the estimation coverage is expanded, this condition
	// will become less strict.
	if !stmt.Exact && stmt.Sources == nil && stmt.Condition == nil && stmt.Dimensions == nil && stmt.Limit == 0 && stmt.Offset == 0 {
		return stmt, nil
	}

	// Check for time in WHERE clause (not supported).
	if HasTimeExpr(stmt.Condition) {
		return nil, errors.New("SHOW MEASUREMENT EXACT CARDINALITY doesn't support time in WHERE clause")
	}

	// Use all measurements, if zero.
	if len(stmt.Sources) == 0 {
		stmt.Sources = Sources{
			&Measurement{Regex: &RegexLiteral{Val: regexp.MustCompile(`.+`)}},
		}
	}

	return &SelectStatement{
		Fields: []*Field{
			{
				Expr: &Call{
					Name: "count",
					Args: []Expr{
						&Call{
							Name: "distinct",
							Args: []Expr{&VarRef{Val: "_name"}},
						},
					},
				},
				Alias: "count",
			},
		},
		Sources:    rewriteSources2(stmt.Sources, stmt.Database),
		Condition:  stmt.Condition,
		Dimensions: stmt.Dimensions,
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		OmitTime:   true,
		StripName:  true,
	}, nil
}

func rewriteShowSeriesStatement(stmt *ShowSeriesStatement) (Statement, error) {
	s := &SelectStatement{
		Condition:  stmt.Condition,
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		SortFields: stmt.SortFields,
		OmitTime:   true,
		StripName:  true,
		Dedupe:     true,
		IsRawQuery: true,
	}
	// Check if we can exclusively use the index.
	if !HasTimeExpr(stmt.Condition) {
		s.Fields = []*Field{{Expr: &VarRef{Val: "key"}}}
		s.Sources = rewriteSources(stmt.Sources, "_series", stmt.Database)
		s.Condition = rewriteSourcesCondition(s.Sources, s.Condition)
		return s, nil
	}

	// The query is bounded by time then it will have to query TSM data rather
	// than utilising the index via system iterators.
	s.Fields = []*Field{
		{Expr: &VarRef{Val: "_seriesKey"}, Alias: "key"},
	}
	s.Sources = rewriteSources2(stmt.Sources, stmt.Database)
	return s, nil
}

func rewriteShowSeriesCardinalityStatement(stmt *ShowSeriesCardinalityStatement) (Statement, error) {
	// TODO(edd): currently we only support cardinality estimation for certain
	// types of query. As the estimation coverage is expanded, this condition
	// will become less strict.
	if !stmt.Exact && stmt.Sources == nil && stmt.Condition == nil && stmt.Dimensions == nil && stmt.Limit == 0 && stmt.Offset == 0 {
		return stmt, nil
	}

	// Check for time in WHERE clause (not supported).
	if HasTimeExpr(stmt.Condition) {
		return nil, errors.New("SHOW SERIES EXACT CARDINALITY doesn't support time in WHERE clause")
	}

	// Use all measurements, if zero.
	if len(stmt.Sources) == 0 {
		stmt.Sources = Sources{
			&Measurement{Regex: &RegexLiteral{Val: regexp.MustCompile(`.+`)}},
		}
	}

	return &SelectStatement{
		Fields: []*Field{
			{Expr: &Call{Name: "count", Args: []Expr{&VarRef{Val: "_seriesKey"}}}, Alias: "count"},
		},
		Sources:    rewriteSources2(stmt.Sources, stmt.Database),
		Condition:  stmt.Condition,
		Dimensions: stmt.Dimensions,
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		OmitTime:   true,
	}, nil
}

func rewriteShowTagValuesStatement(stmt *ShowTagValuesStatement) (Statement, error) {
	// Check for time in WHERE clause (not supported).
	if HasTimeExpr(stmt.Condition) {
		return nil, errors.New("SHOW TAG VALUES doesn't support time in WHERE clause")
	}

	var expr Expr
	if list, ok := stmt.TagKeyExpr.(*ListLiteral); ok {
		for _, tagKey := range list.Vals {
			tagExpr := &BinaryExpr{
				Op:  EQ,
				LHS: &VarRef{Val: "_tagKey"},
				RHS: &StringLiteral{Val: tagKey},
			}

			if expr != nil {
				expr = &BinaryExpr{
					Op:  OR,
					LHS: expr,
					RHS: tagExpr,
				}
			} else {
				expr = tagExpr
			}
		}
	} else {
		expr = &BinaryExpr{
			Op:  stmt.Op,
			LHS: &VarRef{Val: "_tagKey"},
			RHS: stmt.TagKeyExpr,
		}
	}

	// Set condition or "AND" together.
	condition := stmt.Condition
	if condition == nil {
		condition = expr
	} else {
		condition = &BinaryExpr{
			Op:  AND,
			LHS: &ParenExpr{Expr: condition},
			RHS: &ParenExpr{Expr: expr},
		}
	}
	condition = rewriteSourcesCondition(stmt.Sources, condition)

	return &ShowTagValuesStatement{
		Database:   stmt.Database,
		Op:         stmt.Op,
		TagKeyExpr: stmt.TagKeyExpr,
		Condition:  condition,
		SortFields: stmt.SortFields,
		Limit:      stmt.Limit,
		Offset:     stmt.Offset,
	}, nil
}

func rewriteShowTagValuesCardinalityStatement(stmt *ShowTagValuesCardinalityStatement) (Statement, error) {
	// Use all measurements, if zero.
	if len(stmt.Sources) == 0 {
		stmt.Sources = Sources{
			&Measurement{Regex: &RegexLiteral{Val: regexp.MustCompile(`.+`)}},
		}
	}

	var expr Expr
	if list, ok := stmt.TagKeyExpr.(*ListLiteral); ok {
		for _, tagKey := range list.Vals {
			tagExpr := &BinaryExpr{
				Op:  EQ,
				LHS: &VarRef{Val: "_tagKey"},
				RHS: &StringLiteral{Val: tagKey},
			}

			if expr != nil {
				expr = &BinaryExpr{
					Op:  OR,
					LHS: expr,
					RHS: tagExpr,
				}
			} else {
				expr = tagExpr
			}
		}
	} else {
		expr = &BinaryExpr{
			Op:  stmt.Op,
			LHS: &VarRef{Val: "_tagKey"},
			RHS: stmt.TagKeyExpr,
		}
	}

	// Set condition or "AND" together.
	condition := stmt.Condition
	if condition == nil {
		condition = expr
	} else {
		condition = &BinaryExpr{
			Op:  AND,
			LHS: &ParenExpr{Expr: condition},
			RHS: &ParenExpr{Expr: expr},
		}
	}

	return &SelectStatement{
		Fields: []*Field{
			{
				Expr: &Call{
					Name: "count",
					Args: []Expr{
						&Call{
							Name: "distinct",
							Args: []Expr{&VarRef{Val: "_tagValue"}},
						},
					},
				},
				Alias: "count",
			},
		},
		Sources:    rewriteSources2(stmt.Sources, stmt.Database),
		Condition:  condition,
		Dimensions: stmt.Dimensions,
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		OmitTime:   true,
	}, nil
}

func rewriteShowTagKeysStatement(stmt *ShowTagKeysStatement) (Statement, error) {
	s := &SelectStatement{
		Condition:  stmt.Condition,
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		SortFields: stmt.SortFields,
		OmitTime:   true,
		Dedupe:     true,
		IsRawQuery: true,
	}

	// Check if we can exclusively use the index.
	if !HasTimeExpr(stmt.Condition) {
		s.Fields = []*Field{{Expr: &VarRef{Val: "tagKey"}}}
		s.Sources = rewriteSources(stmt.Sources, "_tagKeys", stmt.Database)
		s.Condition = rewriteSourcesCondition(s.Sources, stmt.Condition)
		return s, nil
	}

	// The query is bounded by time then it will have to query TSM data rather
	// than utilising the index via system iterators.
	s.Fields = []*Field{
		{
			Expr: &Call{
				Name: "distinct",
				Args: []Expr{&VarRef{Val: "_tagKey"}},
			},
			Alias: "tagKey",
		},
	}

	s.Sources = rewriteSources2(stmt.Sources, stmt.Database)
	return s, nil
}

func rewriteShowTagKeyCardinalityStatement(stmt *ShowTagKeyCardinalityStatement) (Statement, error) {
	// Check for time in WHERE clause (not supported).
	if HasTimeExpr(stmt.Condition) {
		return nil, errors.New("SHOW TAG KEY EXACT CARDINALITY doesn't support time in WHERE clause")
	}

	// Use all measurements, if zero.
	if len(stmt.Sources) == 0 {
		stmt.Sources = Sources{
			&Measurement{Regex: &RegexLiteral{Val: regexp.MustCompile(`.+`)}},
		}
	}

	return &SelectStatement{
		Fields: []*Field{
			{
				Expr: &Call{
					Name: "count",
					Args: []Expr{
						&Call{
							Name: "distinct",
							Args: []Expr{&VarRef{Val: "_tagKey"}},
						},
					},
				},
				Alias: "count",
			},
		},
		Sources:    rewriteSources2(stmt.Sources, stmt.Database),
		Condition:  stmt.Condition,
		Dimensions: stmt.Dimensions,
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		OmitTime:   true,
	}, nil
}

// rewriteSources rewrites sources to include the provided system iterator.
//
// rewriteSources also sets the default database where necessary.
func rewriteSources(sources Sources, systemIterator, defaultDatabase string) Sources {
	newSources := Sources{}
	for _, src := range sources {
		if src == nil {
			continue
		}
		mm := src.(*Measurement)
		database := mm.Database
		if database == "" {
			database = defaultDatabase
		}

		newM := mm.Clone()
		newM.SystemIterator, newM.Database = systemIterator, database
		newSources = append(newSources, newM)
	}

	if len(newSources) <= 0 {
		return append(newSources, &Measurement{
			Database:       defaultDatabase,
			SystemIterator: systemIterator,
		})
	}
	return newSources
}

// rewriteSourcesCondition rewrites sources into `name` expressions.
// Merges with cond and returns a new condition.
func rewriteSourcesCondition(sources Sources, cond Expr) Expr {
	if len(sources) == 0 {
		return cond
	}

	// Generate an OR'd set of filters on source name.
	var scond Expr
	for _, source := range sources {
		mm := source.(*Measurement)

		// Generate a filtering expression on the measurement name.
		var expr Expr
		if mm.Regex != nil {
			expr = &BinaryExpr{
				Op:  EQREGEX,
				LHS: &VarRef{Val: "_name"},
				RHS: &RegexLiteral{Val: mm.Regex.Val},
			}
		} else if mm.Name != "" {
			expr = &BinaryExpr{
				Op:  EQ,
				LHS: &VarRef{Val: "_name"},
				RHS: &StringLiteral{Val: mm.Name},
			}
		}

		if scond == nil {
			scond = expr
		} else {
			scond = &BinaryExpr{
				Op:  OR,
				LHS: scond,
				RHS: expr,
			}
		}
	}

	// This is the case where the original query has a WHERE on a tag, and also
	// is requesting from a specific source.
	if cond != nil && scond != nil {
		return &BinaryExpr{
			Op:  AND,
			LHS: &ParenExpr{Expr: scond},
			RHS: &ParenExpr{Expr: cond},
		}
	} else if cond != nil {
		// This is the case where the original query has a WHERE on a tag but
		// is not requesting from a specific source.
		return cond
	}
	return scond
}

func rewriteSources2(sources Sources, database string) Sources {
	if len(sources) == 0 {
		sources = Sources{&Measurement{Regex: &RegexLiteral{Val: matchAllRegex.Copy()}}}
	}
	for _, source := range sources {
		switch source := source.(type) {
		case *Measurement:
			if source.Database == "" {
				source.Database = database
			}
		}
	}
	return sources
}

var matchAllRegex = regexp.MustCompile(`.+`)
