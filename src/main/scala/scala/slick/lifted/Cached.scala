package scala.slick.lifted

import scala.language.implicitConversions
import scala.annotation.implicitNotFound
import scala.slick.ast.Node
import scala.slick.profile.BasicProfile

/** A possibly parameterized query that will be cached for repeated efficient
  * execution without having to recompile it every time. The compiled state
  * is computed on demand the first time a `Cached` value is executed. It is
  * always tied to a specific driver.
  *
  * `Cached` forms a limited monad which ensures that it can only contain
  * values that are `Cachable`. */
sealed trait Cached[T] {
  /** The driver which is used for compiling the query. */
  def driver: BasicProfile

  /** Perform a transformation of the underlying value. The computed value must
    * be `Cachable`. The resulting `Cached` instance will be recompiled when
    * needed. It does not benefit from this instance already containing the
    * compiled state. */
  def map[U, C <: Cached[U]](f: T => U)(implicit ucachable: Cachable[U, C]): C =
    ucachable.cached(f(extract), driver)

  /** Perform a transformation of the underlying value. The computed `Cached`
    * value is returned unmodified. */
  def flatMap[U <: Cached[_]](f: T => U): U =
    f(extract)

  /** Return the underlying query or query function. It can be safely
    * extracted for reuse without caching. */
  def extract: T
}

object Cached {
  /** Create a new `Cached` value for a raw value that is `Cachable`. */
  @inline def apply[V, C <: Cached[V]](raw: V)(implicit cachable: Cachable[V, C], driver: BasicProfile): C =
    cachable.cached(raw, driver)
}

class CachedFunction[F, PT, PU, R <: Rep[_], RU](val extract: F, val tuple: F => PT => R, val pshape: Shape[ShapeLevel.Columns, PU, PU, PT], val driver: BasicProfile) extends Cached[F] {
  /** Create an applied `Cached` value for this cached function. All applied
    * values share their compilation state with the original cached function. */
  def apply(p: PU) = new AppliedCachedFunction[PU, R, RU](p, this, driver)

  lazy val tree: Node = {
    val params: PT = pshape.buildParams(_.asInstanceOf[PU])
    val result: R = tuple(extract).apply(params)
    driver.queryCompiler.run(result.toNode).tree
  }

  def applied(param: PU): R = tuple(extract).apply(pshape.pack(param))
}

/** A cached value that can be executed to obtain its result. */
trait RunnableCached[R, RU] extends Cached[R] {
  def param: Any
  def tree: Node
}

class AppliedCachedFunction[PU, R <: Rep[_], RU](val param: PU, function: CachedFunction[_, _, PU, R, RU], val driver: BasicProfile) extends RunnableCached[R, RU] {
  lazy val extract: R = function.applied(param)
  def tree = function.tree
}

class CachedExecutable[R <: Rep[_], RU](val extract: R, val driver: BasicProfile) extends RunnableCached[R, RU] {
  def param = ()
  lazy val tree = driver.queryCompiler.run(extract.toNode).tree
}

/** Typeclass for types that can be executed as queries. This encompasses
  * collection-valued (`Query[_, _]`), scalar and record types. This is used
  * as a phantom type for computing the required types. The actual value is
  * always `null`. */
@implicitNotFound("Computation of type ${T} cannot be executed (with result type ${TU})")
trait Executable[T, TU]

object Executable {
  @inline implicit def queryIsExecutable[B, BU]: Executable[Query[B, BU], Seq[BU]] = null
  @inline implicit def scalarIsExecutable[A, AU](implicit shape: Shape[ShapeLevel.Flat, A, AU, A]): Executable[A, AU] = null
}

/** Typeclass for types that can be contained in a `Cached` container. This
  * includes all `Executable` types as well as functions (of any arity) from
  * flat, fully packed parameter types to an `Executable` result type. */
@implicitNotFound("Computation of type ${T} cannot be cached (as type ${C})")
trait Cachable[T, C <: Cached[T]] {
  def cached(raw: T, driver: BasicProfile): C
}

object Cachable extends CachableFunctions {
  implicit def function1IsCachable[A , B <: Rep[_], P, U](implicit ashape: Shape[ShapeLevel.Columns, A, P, A], pshape: Shape[ShapeLevel.Columns, P, P, _], bexe: Executable[B, U]): Cachable[A => B, CachedFunction[A => B, A , P, B, U]] = new Cachable[A => B, CachedFunction[A => B, A, P, B, U]] {
    def cached(raw: A => B, driver: BasicProfile) =
      new CachedFunction[A => B, A, P, B, U](raw, identity[A => B], pshape.asInstanceOf[Shape[ShapeLevel.Columns, P, P, A]], driver)
  }
}

trait CachableLowPriority {
  implicit def executableIsCachable[T <: Rep[_], U](implicit e: Executable[T, U]): Cachable[T, CachedExecutable[T, U]] = new Cachable[T, CachedExecutable[T, U]] {
    def cached(raw: T, driver: BasicProfile) = new CachedExecutable[T, U](raw, driver)
  }
}

final class Parameters[PU, PP](pshape: Shape[ShapeLevel.Columns, PU, PU, _]) {
  def flatMap[R <: Rep[_], RU](f: PP => R)(implicit rexe: Executable[R, RU], driver: BasicProfile): CachedFunction[PP => R, PP, PU, R, RU] =
    new CachedFunction[PP => R, PP, PU, R, RU](f, identity[PP => R], pshape.asInstanceOf[Shape[ShapeLevel.Columns, PU, PU, PP]], driver)

  @inline def withFilter(f: PP => Boolean): Parameters[PU, PP] = this
}

object Parameters {
  @inline def apply[U](implicit pshape: Shape[ShapeLevel.Columns, U, U, _]): Parameters[U, pshape.Packed] =
    new Parameters[U, pshape.Packed](pshape)
}
