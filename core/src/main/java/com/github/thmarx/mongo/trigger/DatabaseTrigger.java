/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Interface.java to edit this template
 */
package com.github.thmarx.mongo.trigger;

import java.util.function.BiConsumer;

/**
 *
 * @author t.marx
 */
@FunctionalInterface
public interface DatabaseTrigger extends BiConsumer<DatabaseTrigger.Type, String>{
	
	public enum Type {
		DROPPED;
	}
}
