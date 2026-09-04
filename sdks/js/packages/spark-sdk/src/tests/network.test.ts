import { describe, expect, it } from "@jest/globals";
import {
  isLightningInvoiceNetworkCompatible,
  Network,
} from "../utils/network.js";

describe("Lightning invoice network compatibility", () => {
  it("accepts the shared testnet prefix for a SIGNET wallet", () => {
    expect(
      isLightningInvoiceNetworkCompatible(Network.TESTNET, Network.SIGNET),
    ).toBe(true);
  });

  it("keeps distinct networks isolated", () => {
    expect(
      isLightningInvoiceNetworkCompatible(Network.MAINNET, Network.SIGNET),
    ).toBe(false);
    expect(
      isLightningInvoiceNetworkCompatible(Network.REGTEST, Network.SIGNET),
    ).toBe(false);
    expect(
      isLightningInvoiceNetworkCompatible(Network.TESTNET, Network.MAINNET),
    ).toBe(false);
  });

  it("continues to accept LOCAL regtest invoices", () => {
    expect(
      isLightningInvoiceNetworkCompatible(Network.REGTEST, Network.LOCAL),
    ).toBe(true);
  });
});
