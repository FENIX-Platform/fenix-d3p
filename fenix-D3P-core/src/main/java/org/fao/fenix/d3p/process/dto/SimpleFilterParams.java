package org.fao.fenix.d3p.process.dto;

import org.fao.fenix.commons.find.dto.filter.DataFilter;
import org.fao.fenix.commons.utils.Order;

public class SimpleFilterParams {
    Order order;
    DataFilter filter;


    public Order getOrder() {
        return order;
    }

    public void setOrder(Order order) {
        this.order = order;
    }

    public DataFilter getFilter() {
        return filter;
    }

    public void setFilter(DataFilter filter) {
        this.filter = filter;
    }
}
