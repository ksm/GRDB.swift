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
    
    func joining<A: Association>(_ joinOperator: JoinOperator, _ association: A) -> JoinQuery {
        let join = Join(
            joinOperator: joinOperator,
            joinCondition: association.joinCondition,
            query: association.query)
        return joining(join, forKey: association.key)
    }
    
    private func joining(_ join: Join, forKey key: String) -> JoinQuery {
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
