# Timeout requirements follow-up

Follow-up assessment for [issue #18322](https://github.com/infinispan/infinispan/issues/18322)
and the [requirements table on PR #18333](https://github.com/infinispan/infinispan/pull/18333#issuecomment-7016973493).
Requirement numbers below refer to that table.
The implementation changes are committed in `c3e06a13c1` (`[#18322] Fix remaining timeout validation gaps`).
The proposed issue updates below have not been applied to GitHub.

| Requirement | Why unchanged | What should happen next |
|---|---|---|
| **#3: Synchronous backup timeout ordering** | The table asks for backup timeout **above** lock timeout, but the issue describes problems caused by exactly that relationship. Existing code warns when backup timeout is greater than or equal to a positive lock timeout. | **Correct the issue description and investigate before enforcing more.** State that backup should finish before the originating remote call expires. Treat the lock relationship as advisory. Trace lock lifetime and cumulative RPC delays for transactional and nontransactional writes, then use clustered cross-site tests to establish any stronger constraint. |
| **#7: Cache-stop timeout above remote/lock timeouts** | Existing warnings identify short shutdown waits while preserving intentional bounded shutdown, including zero. Making this mandatory would remove that choice. | **Make the policy explicit in the issue:** require nonnegative values and warn below applicable operation timeouts. If strict enforcement is desired, that needs an explicit compatibility decision. Shutdown tests with active local and remote transactions would provide stronger behavioral coverage. |
| **#10: Cross-site state-transfer timeout above retry wait time** | The timeout applies to each attempt. Backoff happens after failure; each retry gets a fresh timeout. There is no shared budget that the backoff consumes. | **Remove this ordering requirement and correct the explanation.** Keep both values positive and independent. Existing parsing tests and documentation cover equal or longer backoff. No further investigation is needed to reject this particular constraint. |
| **#11: JKU timeout above connection + read timeouts** | JKU timeout controls cached-key lifetime, rather than an overall fetch deadline. Documentation was clarified in the implementation commit. | **Remove this ordering requirement.** Describe key-cache lifetime separately from network timeouts. If an overall fetch deadline is wanted, investigate it as a separate feature in the Elytron integration. |

Requirements **#1, #4, #5, #6, #8, and #9** needed no additional implementation because the PR already handles them.
The issue should distinguish the accepted policies: equality warns for lock/remote and completed-transaction ordering, whereas equality fails for finite state-transfer and distributed-sync timeouts.
Nonpositive lock-acquisition timeouts mean immediate acquisition attempts, not infinite waits.
Requirement **#2** received an additional fix: Hot Rod server configuration now rejects nonpositive topology replication timeouts before creating the internal cache.

The recommended next steps are to correct issue #18322, explicitly settle the warning policy for #7, and keep #3 as the main remaining technical investigation.

Evidence for the timeout semantics:

- Lock acquisition: `core/src/main/java/org/infinispan/util/concurrent/locks/impl/DefaultLockManager.java`, `KeyAwareExtendedLockPromise.scheduleLockTimeoutTask()`.
- Shutdown waiting: `core/src/main/java/org/infinispan/transaction/impl/TransactionTable.java`, `shutDownGracefully()`.
- Cross-site retries: `core/src/main/java/org/infinispan/xsite/statetransfer/SyncProviderState.java`, `CommandRetry`.
- JKU cached-key lifetime: [Elytron 2.9.4.Final JwkManager](https://github.com/wildfly-security/wildfly-elytron/blob/2.9.4.Final/auth/realm/token/src/main/java/org/wildfly/security/auth/realm/token/validator/JwkManager.java).
