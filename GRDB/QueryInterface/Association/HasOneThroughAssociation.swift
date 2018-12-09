public struct HasOneThroughAssociation<Origin, Destination>: ToOneAssociation {
    /// :nodoc:
    public typealias OriginRowDecoder = Origin
    
    /// :nodoc:
    public typealias RowDecoder = Destination

    /// :nodoc:
    public var _impl: _HasOneThroughAssociationImpl
}

/// [**Experimental**](http://github.com/groue/GRDB.swift#what-are-experimental-features)
///
/// :nodoc:
public struct _HasOneThroughAssociationImpl: _AssociationImpl {
    var pivot: _AssociationImpl
    var target: _AssociationImpl
    
    public var key: String {
        get { return target.key }
        set { target.key = newValue }
    }
    
    public func mapQuery(_ transform: (JoinQuery) -> JoinQuery) -> _HasOneThroughAssociationImpl {
        var impl = self
        impl.target = impl.target.mapQuery(transform)
        return impl
    }
    
    public func joinedRequest<T>(_ request: QueryInterfaceRequest<T>, joinOperator: JoinOperator) -> QueryInterfaceRequest<T> {
        return pivot
            .mapQuery { target.joinedQuery($0.select([]), joinOperator: joinOperator) }
            .joinedRequest(request, joinOperator: joinOperator)
    }
    
    public func joinedQuery(_ query: JoinQuery, joinOperator: JoinOperator) -> JoinQuery {
        return pivot
            .mapQuery { target.joinedQuery($0.select([]), joinOperator: joinOperator) }
            .joinedQuery(query, joinOperator: joinOperator)
    }
}

extension TableRecord {
    public static func hasOne<Pivot, Target>(
        _ target: Target,
        through pivot: Pivot)
        -> HasOneThroughAssociation<Self, Target.RowDecoder>
        where Pivot: ToOneAssociation,
        Target: ToOneAssociation,
        Pivot.OriginRowDecoder == Self,
        Pivot.RowDecoder == Target.OriginRowDecoder
    {
        return HasOneThroughAssociation(_impl: _HasOneThroughAssociationImpl(
            pivot: pivot._impl,
            target: target._impl))
    }
}
