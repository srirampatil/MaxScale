#ifndef _GWBITMASK_H
#define _GWBITMASK_H
/*
 * This file is distributed as part of the MariaDB Corporation MaxScale.  It is free
 * software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation,
 * version 2.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 51
 * Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Copyright MariaDB Corporation Ab 2013-2014
 */
#include <spinlock.h>

/**
 * @file gwbitmask.h An implementation of an arbitarly long bitmask
 *
 * @verbatim
 * Revision History
 *
 * Date		Who		Description
 * 28/06/13	Mark Riddoch	Initial implementation
 *
 * @endverbatim
 */

#define BIT_LENGTH_INITIAL	32	/**< Initial number of bits in the bitmask */
#define BIT_LENGTH_INC		32	/**< Number of bits to add on each increment */

/**
 * The bitmask structure used to store an arbitary large bitmask
 */
typedef struct {
	SPINLOCK	lock;		/**< Lock to protect the bitmask */
	unsigned char	*bits;		/**< Pointer to the bits themselves */
	unsigned int	length;		/**< The number of bits in the bitmask */
} GWBITMASK;

extern void bitmask_init(GWBITMASK *);
extern void bitmask_free(GWBITMASK *);
extern void bitmask_set(GWBITMASK *, int);
extern void bitmask_clear(GWBITMASK *, int);
extern int  bitmask_isset(GWBITMASK *, int);
extern int  bitmask_isallclear(GWBITMASK *);
extern void bitmask_copy(GWBITMASK *, GWBITMASK *);
#endif
