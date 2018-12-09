// MARK: - JoinKind

/// [**Experimental**](http://github.com/groue/GRDB.swift#what-are-experimental-features)
///
/// JoinKind is not to be mismatched with SQL join operators (inner join,
/// left join).
///
/// JoinKind is designed to be hierarchically nested, unlike
/// SQL join operators.
///
/// Consider the following request for (A, B, C) tuples:
///
///     let r = A.including(optional: A.b.including(required: B.c))
///
/// It chains two associations, the first optional, the second required.
///
/// It looks like it means: "Give me all As, along with their Bs, granted those
/// Bs have their Cs. For As whose B has no C, give me a nil B".
///
/// It can not be expressed as one left join, and a regular join, as below,
/// Because this would not honor the first optional:
///
///     -- dubious
///     SELECT a.*, b.*, c.*
///     FROM a
///     LEFT JOIN b ON ...
///     JOIN c ON ...
///
/// Instead, it should:
/// - allow (A + missing (B + C))
/// - prevent (A + (B + missing C)).
///
/// This can be expressed in SQL with two left joins, and an extra condition:
///
///     -- likely correct
///     SELECT a.*, b.*, c.*
///     FROM a
///     LEFT JOIN b ON ...
///     LEFT JOIN c ON ...
///     WHERE NOT((b.id IS NOT NULL) AND (c.id IS NULL)) -- no B without C
///
/// This is currently not implemented, and requires a little more thought.
/// I don't even know if inventing a whole new way to perform joins should even
/// be on the table. But we have a hierarchical way to express joined queries,
/// and they have a meaning:
///
///     // what is my meaning?
///     A.including(optional: A.b.including(required: B.c))
/// TODO: Hide if possible
public enum JoinKind {
    case required, optional
}

// MARK: - JoinCondition

/// The condition that links two joined tables.
///
/// We only support one kind of join condition, today: foreign keys.
///
///     SELECT ...
///     FROM book
///     JOIN author ON author.id = book.authorId
///                    <--the join condition--->
///
/// When we eventually add support for new ways to join tables, JoinCondition
/// is the type we'll need to update.
///
/// The Equatable conformance is used when we merge associations. Two
/// associations can be merged if and only if their join conditions
/// are equal:
///
///     let request = Book
///         .include(required: Book.author)
///         .include(required: Book.author)
struct JoinCondition: Equatable {
    var foreignKeyRequest: ForeignKeyRequest
    var originIsLeft: Bool
    
    func sqlExpression(_ db: Database, leftAlias: TableAlias, rightAlias: TableAlias) throws -> SQLExpression {
        let foreignKeyMapping = try foreignKeyRequest.fetch(db).mapping
        let columnMapping: [(left: Column, right: Column)]
        if originIsLeft {
            columnMapping = foreignKeyMapping.map { (left: Column($0.origin), right: Column($0.destination)) }
        } else {
            columnMapping = foreignKeyMapping.map { (left: Column($0.destination), right: Column($0.origin)) }
        }
        
        return columnMapping
            .map { $0.right.qualifiedExpression(with: rightAlias) == $0.left.qualifiedExpression(with: leftAlias) }
            .joined(operator: .and)
    }
}

// MARK: - Join

struct Join {
    var kind: JoinKind
    var condition: JoinCondition
    var query: JoinQuery
    
    var finalizedJoin: Join {
        var join = self
        join.query = query.finalizedQuery
        return join
    }
    
    var finalizedAliases: [TableAlias] {
        return query.finalizedAliases
    }
    
    var finalizedSelection: [SQLSelectable] {
        return query.finalizedSelection
    }
    
    var finalizedOrdering: QueryOrdering {
        return query.finalizedOrdering
    }
    
    func finalizedRowAdapter(_ db: Database, fromIndex startIndex: Int, forKeyPath keyPath: [String]) throws -> (adapter: RowAdapter, endIndex: Int)? {
        return try query.finalizedRowAdapter(db, fromIndex: startIndex, forKeyPath: keyPath)
    }
    
    /// precondition: query is the result of finalizedQuery
    func joinSQL(_ db: Database,_ context: inout SQLGenerationContext, leftAlias: TableAlias, isRequiredAllowed: Bool) throws -> String {
        var isRequiredAllowed = isRequiredAllowed
        var sql = ""
        switch kind {
        case .optional:
            isRequiredAllowed = false
            sql += "LEFT JOIN"
        case .required:
            guard isRequiredAllowed else {
                // TODO: chainOptionalRequired
                fatalError("Not implemented: chaining a required association behind an optional association")
            }
            sql += "JOIN"
        }
        
        sql += try " " + query.source.sourceSQL(db, &context)
        
        let rightAlias = query.alias!
        var filters = try [condition.sqlExpression(db, leftAlias: leftAlias, rightAlias: rightAlias)]
        if let filter = try query.filterPromise.resolve(db) {
            filters.append(filter)
        }
        sql += " ON " + filters.joined(operator: .and).expressionSQL(&context)
        
        for (_, join) in query.joins {
            sql += try " " + join.joinSQL(db, &context, leftAlias: rightAlias, isRequiredAllowed: isRequiredAllowed)
        }
        
        return sql
    }
    
    /// Returns nil if joins can't be merged (conflict in condition, query...)
    func merged(with other: Join) -> Join? {
        guard condition == other.condition else {
            // can't merge
            return nil
        }
        
        guard let mergedQuery = query.merged(with: other.query) else {
            // can't merge
            return nil
        }
        
        let mergedKind: JoinKind
        switch (kind, other.kind) {
        case (.required, _), (_, .required):
            mergedKind = .required
        default:
            mergedKind = .optional
        }
        
        return Join(
            kind: mergedKind,
            condition: condition,
            query: mergedQuery)
    }
}

// MARK: - JoinQuery

/// [**Experimental**](http://github.com/groue/GRDB.swift#what-are-experimental-features)
/// TODO: Hide if possible
public struct JoinQuery {
    var source: SQLSource
    var selection: [SQLSelectable]
    var filterPromise: DatabasePromise<SQLExpression?>
    var ordering: QueryOrdering
    var joins: OrderedDictionary<String, Join>
    
    var alias: TableAlias? {
        return source.alias
    }
}

extension JoinQuery {
    init(_ query: QueryInterfaceQuery) {
        GRDBPrecondition(!query.isDistinct, "Not implemented: join distinct queries")
        GRDBPrecondition(query.groupPromise == nil, "Can't join aggregated queries")
        GRDBPrecondition(query.havingExpression == nil, "Can't join aggregated queries")
        GRDBPrecondition(query.limit == nil, "Can't join limited queries")
        
        self.init(
            source: query.source,
            selection: query.selection,
            filterPromise: query.filterPromise,
            ordering: query.ordering,
            joins: query.joins)
    }
}

extension JoinQuery {
    func select(_ selection: [SQLSelectable]) -> JoinQuery {
        var query = self
        query.selection = selection
        return query
    }
    
    func filter(_ predicate: @escaping (Database) throws -> SQLExpressible) -> JoinQuery {
        var query = self
        query.filterPromise = query.filterPromise.map { (db, filter) in
            if let filter = filter {
                return try filter && predicate(db)
            } else {
                return try predicate(db).sqlExpression
            }
        }
        return query
    }
    
    func order(_ orderings: @escaping (Database) throws -> [SQLOrderingTerm]) -> JoinQuery {
        return order(QueryOrdering(orderings: orderings))
    }
    
    func reversed() -> JoinQuery {
        return order(ordering.reversed)
    }
    
    private func order(_ ordering: QueryOrdering) -> JoinQuery {
        var query = self
        query.ordering = ordering
        return query
    }
    
    func joined(with join: Join, on key: String) -> JoinQuery {
        var query = self
        if let existingJoin = query.joins.removeValue(forKey: key) {
            guard let mergedJoin = existingJoin.merged(with: join) else {
                // can't merge
                fatalError("The association key \"\(key)\" is ambiguous. Use the Association.forKey(_:) method is order to disambiguate.")
            }
            query.joins.append(value: mergedJoin, forKey: key)
        } else {
            query.joins.append(value: join, forKey: key)
        }
        return query
    }
    
    func qualified(with alias: TableAlias) -> JoinQuery {
        var query = self
        query.source = source.qualified(with: alias)
        return query
    }
}

extension JoinQuery {
    /// A finalized query is ready for SQL generation
    var finalizedQuery: JoinQuery {
        var query = self
        
        let alias = TableAlias()
        query.source = source.qualified(with: alias)
        query.selection = selection.map { $0.qualifiedSelectable(with: alias) }
        query.filterPromise = filterPromise.map { [alias] (_, expr) in expr?.qualifiedExpression(with: alias) }
        query.ordering = ordering.qualified(with: alias)
        query.joins = joins.mapValues { $0.finalizedJoin }
        
        return query
    }
    
    /// precondition: self is the result of finalizedQuery
    var finalizedAliases: [TableAlias] {
        var aliases: [TableAlias] = []
        if let alias = alias {
            aliases.append(alias)
        }
        return joins.reduce(into: aliases) {
            $0.append(contentsOf: $1.value.finalizedAliases)
        }
    }
    
    /// precondition: self is the result of finalizedQuery
    var finalizedSelection: [SQLSelectable] {
        return joins.reduce(into: selection) {
            $0.append(contentsOf: $1.value.finalizedSelection)
        }
    }
    
    /// precondition: self is the result of finalizedQuery
    var finalizedOrdering: QueryOrdering {
        return joins.reduce(ordering) {
            $0.appending($1.value.finalizedOrdering)
        }
    }
    
    /// precondition: self is the result of finalizedQuery
    func finalizedRowAdapter(_ db: Database, fromIndex startIndex: Int, forKeyPath keyPath: [String]) throws -> (adapter: RowAdapter, endIndex: Int)? {
        let selectionWidth = try selection
            .map { try $0.columnCount(db) }
            .reduce(0, +)
        
        var endIndex = startIndex + selectionWidth
        var scopes: [String: RowAdapter] = [:]
        for (key, join) in joins {
            if let (joinAdapter, joinEndIndex) = try join.finalizedRowAdapter(db, fromIndex: endIndex, forKeyPath: keyPath + [key]) {
                scopes[key] = joinAdapter
                endIndex = joinEndIndex
            }
        }
        
        if selectionWidth == 0 && scopes.isEmpty {
            return nil
        }
        
        let adapter = RangeRowAdapter(startIndex ..< (startIndex + selectionWidth))
        return (adapter: adapter.addingScopes(scopes), endIndex: endIndex)
    }
}

extension JoinQuery {
    /// Returns nil if queries can't be merged (conflict in source, joins...)
    func merged(with other: JoinQuery) -> JoinQuery? {
        guard let mergedSource = source.merged(with: other.source) else {
            // can't merge
            return nil
        }
        
        let mergedFilterPromise = filterPromise.map { (db, expression) in
            let otherExpression = try other.filterPromise.resolve(db)
            let expressions = [expression, otherExpression].compactMap { $0 }
            if expressions.isEmpty {
                return nil
            }
            return expressions.joined(operator: .and)
        }
        
        var mergedJoins: OrderedDictionary<String, Join> = [:]
        for (key, join) in joins {
            if let otherJoin = other.joins[key] {
                guard let mergedJoin = join.merged(with: otherJoin) else {
                    // can't merge
                    return nil
                }
                mergedJoins.append(value: mergedJoin, forKey: key)
            } else {
                mergedJoins.append(value: join, forKey: key)
            }
        }
        for (key, join) in other.joins where mergedJoins[key] == nil {
            mergedJoins.append(value: join, forKey: key)
        }
        
        // replace selection unless empty
        let mergedSelection = other.selection.isEmpty ? selection : other.selection
        
        // replace ordering unless empty
        let mergedOrdering = other.ordering.isEmpty ? ordering : other.ordering
        
        return JoinQuery(
            source: mergedSource,
            selection: mergedSelection,
            filterPromise: mergedFilterPromise,
            ordering: mergedOrdering,
            joins: mergedJoins)
    }
}
