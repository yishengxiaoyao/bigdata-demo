package com.allaboutscala.chapter5.di

trait DonutShoppingCartServices[A] extends DonutShoppingCartDao[A] with DonutInventoryService[A] {
 override val donutDatabase: DonutDatabase[A] = new CassandraDonutStore[A]()
}