﻿// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Security.Cryptography;
using Microsoft.Data.SqlClient.AlwaysEncrypted.AzureKeyVaultProvider;
using Microsoft.Data.SqlClient.ManualTesting.Tests.AlwaysEncrypted.Setup;
using Xunit;

namespace Microsoft.Data.SqlClient.ManualTesting.Tests.AlwaysEncrypted
{
    public class ExceptionTestAKVStore : IClassFixture<SQLSetupStrategyAzureKeyVault>
    {
        private const string MasterKeyEncAlgo = "RSA_OAEP";
        private const string BadMasterKeyEncAlgo = "BadMasterKeyAlgorithm";

        private SQLSetupStrategyAzureKeyVault fixture;

        private byte[] cek;
        private byte[] encryptedCek;

        public ExceptionTestAKVStore(SQLSetupStrategyAzureKeyVault fixture)
        {
            this.fixture = fixture;
            cek = ColumnEncryptionKey.GenerateRandomBytes(ColumnEncryptionKey.KeySizeInBytes);
            encryptedCek = fixture.AkvStoreProvider.EncryptColumnEncryptionKey(DataTestUtility.AKVUrl, MasterKeyEncAlgo, cek);

            // Disable the cache to avoid false failures.
            SqlConnection.ColumnEncryptionQueryMetadataCacheEnabled = false;
        }

        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.IsAKVSetupAvailable))]
        public void InvalidKeyEncryptionAlgorithm()
        {
            Exception ex1 = Assert.Throws<ArgumentException>(() => fixture.AkvStoreProvider.DecryptColumnEncryptionKey(DataTestUtility.AKVUrl, BadMasterKeyEncAlgo, cek));
            Assert.Matches($@"Invalid key encryption algorithm specified: 'BadMasterKeyAlgorithm'. Expected value: 'RSA_OAEP' or 'RSA-OAEP'.\s+\(?Parameter (name: )?'?encryptionAlgorithm('\))?", ex1.Message);

            Exception ex2 = Assert.Throws<ArgumentException>(() => fixture.AkvStoreProvider.EncryptColumnEncryptionKey(DataTestUtility.AKVUrl, BadMasterKeyEncAlgo, cek));
            Assert.Matches($@"Invalid key encryption algorithm specified: 'BadMasterKeyAlgorithm'. Expected value: 'RSA_OAEP' or 'RSA-OAEP'.\s+\(?Parameter (name: )?'?encryptionAlgorithm('\))?", ex2.Message);
        }

        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.IsAKVSetupAvailable))]
        public void NullEncryptionAlgorithm()
        {
            Exception ex1 = Assert.Throws<ArgumentNullException>(() => fixture.AkvStoreProvider.DecryptColumnEncryptionKey(DataTestUtility.AKVUrl, null, cek));
            Assert.Matches($@"Internal error. Key encryption algorithm cannot be null.\s+\(?Parameter (name: )?'?encryptionAlgorithm('\))?", ex1.Message);
            Exception ex2 = Assert.Throws<ArgumentNullException>(() => fixture.AkvStoreProvider.EncryptColumnEncryptionKey(DataTestUtility.AKVUrl, null, cek));
            Assert.Matches($@"Internal error. Key encryption algorithm cannot be null.\s+\(?Parameter (name: )?'?encryptionAlgorithm('\))?", ex2.Message);
        }


        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.IsAKVSetupAvailable))]
        public void EmptyColumnEncryptionKey()
        {
            Exception ex1 = Assert.Throws<ArgumentException>(() => fixture.AkvStoreProvider.EncryptColumnEncryptionKey(DataTestUtility.AKVUrl, MasterKeyEncAlgo, new byte[] { }));
            Assert.Matches($@"Internal error. Empty columnEncryptionKey specified.", ex1.Message);
        }

        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.IsAKVSetupAvailable))]
        public void NullColumnEncryptionKey()
        {
            Exception ex1 = Assert.Throws<ArgumentNullException>(() => fixture.AkvStoreProvider.EncryptColumnEncryptionKey(DataTestUtility.AKVUrl, MasterKeyEncAlgo, null));
            Assert.Matches($@"Value cannot be null.\s+\(?Parameter (name: )?'?columnEncryptionKey('\))?", ex1.Message);
        }

        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.IsAKVSetupAvailable))]
        public void EmptyEncryptedColumnEncryptionKey()
        {
            Exception ex1 = Assert.Throws<ArgumentException>(() => fixture.AkvStoreProvider.DecryptColumnEncryptionKey(DataTestUtility.AKVUrl, MasterKeyEncAlgo, new byte[] { }));
            Assert.Matches($@"Internal error. Empty encryptedColumnEncryptionKey specified", ex1.Message);
        }

        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.IsAKVSetupAvailable))]
        public void NullEncryptedColumnEncryptionKey()
        {
            Exception ex1 = Assert.Throws<ArgumentNullException>(() => fixture.AkvStoreProvider.DecryptColumnEncryptionKey(DataTestUtility.AKVUrl, MasterKeyEncAlgo, null));
            Assert.Matches($@"Value cannot be null.\s+\(?Parameter (name: )?'?encryptedColumnEncryptionKey('\))?", ex1.Message);
        }

        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.IsAKVSetupAvailable))]
        public void InvalidAlgorithmVersion()
        {
            byte[] encrypteCekLocal = ColumnEncryptionKey.GenerateInvalidEncryptedCek(encryptedCek, ColumnEncryptionKey.ECEKCorruption.ALGORITHM_VERSION);
            Exception ex1 = Assert.Throws<ArgumentException>(() => fixture.AkvStoreProvider.DecryptColumnEncryptionKey(DataTestUtility.AKVUrl, MasterKeyEncAlgo, encrypteCekLocal));
            Assert.Matches($@"Specified encrypted column encryption key contains an invalid encryption algorithm version '10'. Expected version is '01'.\s+\(?Parameter (name: )?'?encryptedColumnEncryptionKey('\))?", ex1.Message);

            Exception ex2 = Assert.Throws<ArgumentException>(() => fixture.AkvStoreProvider.EncryptColumnEncryptionKey(DataTestUtility.AKVUrl, "RSA_CORRUPT", encryptedCek));
            Assert.Contains("Invalid key encryption algorithm specified: 'RSA_CORRUPT'. Expected value: 'RSA_OAEP' or 'RSA-OAEP'.", ex2.Message);
        }

        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.IsAKVSetupAvailable))]
        public void InvalidCertificateSignature()
        {
            // Put an invalid signature
            byte[] encrypteCekLocal = ColumnEncryptionKey.GenerateInvalidEncryptedCek(encryptedCek, ColumnEncryptionKey.ECEKCorruption.SIGNATURE);
            string errorMessage =
                $@"The specified encrypted column encryption key signature does not match the signature computed with the column master key \(Asymmetric key in Azure Key Vault\) in '{DataTestUtility.AKVUrl}'. The encrypted column encryption key may be corrupt, or the specified path may be incorrect.\s+\(?Parameter (name: )?'?encryptedColumnEncryptionKey('\))?";

            Exception ex1 = Assert.Throws<ArgumentException>(() => fixture.AkvStoreProvider.DecryptColumnEncryptionKey(DataTestUtility.AKVUrl, MasterKeyEncAlgo, encrypteCekLocal));
            Assert.Matches(errorMessage, ex1.Message);
        }

        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.IsAKVSetupAvailable))]
        public void InvalidCipherTextLength()
        {
            // Put an invalid signature
            byte[] encrypteCekLocal = ColumnEncryptionKey.GenerateInvalidEncryptedCek(encryptedCek, ColumnEncryptionKey.ECEKCorruption.CEK_LENGTH);
            string errorMessage = $@"The specified encrypted column encryption key's ciphertext length: 251 does not match the ciphertext length: 256 when using column master key \(Azure Key Vault key\) in '{DataTestUtility.AKVUrl}'. The encrypted column encryption key may be corrupt, or the specified Azure Key Vault key path may be incorrect.\s+\(?Parameter (name: )?'?encryptedColumnEncryptionKey('\))?";

            Exception ex1 = Assert.Throws<ArgumentException>(() => fixture.AkvStoreProvider.DecryptColumnEncryptionKey(DataTestUtility.AKVUrl, MasterKeyEncAlgo, encrypteCekLocal));
            Assert.Matches(errorMessage, ex1.Message);
        }

        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.IsAKVSetupAvailable))]
        public void InvalidSignatureInEncryptedCek()
        {
            byte[] encryptedCekLocal = ColumnEncryptionKey.GenerateInvalidEncryptedCek(encryptedCek, ColumnEncryptionKey.ECEKCorruption.SIGNATURE_LENGTH);
            string errorMessage = $@"The specified encrypted column encryption key's signature length: 249 does not match the signature length: 256 when using column master key \(Azure Key Vault key\) in '{DataTestUtility.AKVUrl}'. The encrypted column encryption key may be corrupt, or the specified Azure Key Vault key path may be incorrect.\s+\(?Parameter (name: )?'?encryptedColumnEncryptionKey('\))?";

            Exception ex1 = Assert.Throws<ArgumentException>(() => fixture.AkvStoreProvider.DecryptColumnEncryptionKey(DataTestUtility.AKVUrl, MasterKeyEncAlgo, encryptedCekLocal));
            Assert.Matches(errorMessage, ex1.Message);
        }

        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.IsAKVSetupAvailable))]
        public void InvalidURL()
        {
            char[] barePath = new char[32780];
            for (int i = 0; i < barePath.Length; i++)
            {
                barePath[i] = 'a';
            }

            string fakePath = new string(barePath);
            string errorMessage = $@"Invalid url specified: '{fakePath}'.\s+\(?Parameter (name: )?'?masterKeyPath('\))?";

            Exception ex1 = Assert.Throws<ArgumentException>(() => fixture.AkvStoreProvider.EncryptColumnEncryptionKey(fakePath, MasterKeyEncAlgo, cek));
            Assert.Matches(errorMessage, ex1.Message);

            Exception ex2 = Assert.Throws<ArgumentException>(() => fixture.AkvStoreProvider.DecryptColumnEncryptionKey(fakePath, MasterKeyEncAlgo, encryptedCek));
            Assert.Matches(errorMessage, ex2.Message);
        }

        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.IsAKVSetupAvailable))]
        public void NullAKVKeyPath()
        {
            Exception ex1 = Assert.Throws<ArgumentNullException>(
                () => fixture.AkvStoreProvider.EncryptColumnEncryptionKey(null, MasterKeyEncAlgo, cek));
            Assert.Matches($@"Azure Key Vault key path cannot be null.\s+\(?Parameter (name: )?'?masterKeyPath('\))?", ex1.Message);

            Exception ex2 = Assert.Throws<ArgumentNullException>(
                () => fixture.AkvStoreProvider.DecryptColumnEncryptionKey(null, MasterKeyEncAlgo, encryptedCek));
            Assert.Matches($@"Internal error. Azure Key Vault key path cannot be null.\s+\(?Parameter (name: )?'?masterKeyPath('\))?", ex2.Message);
        }

        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsAKVSetupAvailable))]
        public void SignInvalidAKVPath(string masterKeyPath)
        {
            Exception ex = Assert.Throws<ArgumentException>(() =>
            {
                SqlColumnEncryptionAzureKeyVaultProvider azureKeyProvider = new SqlColumnEncryptionAzureKeyVaultProvider(
                    new SqlClientCustomTokenCredential());
                azureKeyProvider.SignColumnMasterKeyMetadata(masterKeyPath, false);
            });

            if (masterKeyPath == null)
            {
                Assert.Matches("Internal error. Azure Key Vault key path cannot be null.", ex.Message);
            }
            else
            {
                Assert.Matches("Invalid Azure Key Vault key path specified", ex.Message);
            }
        }

        [ConditionalFact(typeof(DataTestUtility), nameof(DataTestUtility.IsAKVSetupAvailable))]
        public void InvalidCertificatePath()
        {
            string dummyPathWithOnlyHost = @"https://www.microsoft.com";
            string invalidUrlErrorMessage = $@"Invalid url specified: '{dummyPathWithOnlyHost}'";
            string dummyPathWithInvalidKey = @"https://www.microsoft.vault.azure.com/keys/dummykey/dummykeyid";
            string invalidTrustedEndpointErrorMessage = $@"Invalid Azure Key Vault key path specified: '{dummyPathWithInvalidKey}'. Valid trusted endpoints: vault.azure.net, vault.azure.cn, vault.usgovcloudapi.net, vault.microsoftazure.de, managedhsm.azure.net, managedhsm.azure.cn, managedhsm.usgovcloudapi.net, managedhsm.microsoftazure.de.\s+\(?Parameter (name: )?'?masterKeyPath('\))?";

            Exception ex = Assert.Throws<ArgumentException>(
                () => fixture.AkvStoreProvider.EncryptColumnEncryptionKey(dummyPathWithOnlyHost, MasterKeyEncAlgo, cek));
            Assert.Matches(invalidUrlErrorMessage, ex.Message);

            ex = Assert.Throws<ArgumentException>(
                () => fixture.AkvStoreProvider.EncryptColumnEncryptionKey(dummyPathWithInvalidKey, MasterKeyEncAlgo, cek));
            Assert.Matches(invalidTrustedEndpointErrorMessage, ex.Message);

            ex = Assert.Throws<ArgumentException>(
                 () => fixture.AkvStoreProvider.DecryptColumnEncryptionKey(dummyPathWithOnlyHost, MasterKeyEncAlgo, encryptedCek));
            Assert.Matches(invalidUrlErrorMessage, ex.Message);

            ex = Assert.Throws<ArgumentException>(
                 () => fixture.AkvStoreProvider.DecryptColumnEncryptionKey(dummyPathWithInvalidKey, MasterKeyEncAlgo, encryptedCek));
            Assert.Matches(invalidTrustedEndpointErrorMessage, ex.Message);
        }

        [InlineData(true)]
        [InlineData(false)]
        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsAKVSetupAvailable))]
        public void AkvStoreProviderVerifyFunctionWithInvalidSignature(bool fEnclaveEnabled)
        {
            //sign the cmk
            byte[] cmkSignature = fixture.AkvStoreProvider.SignColumnMasterKeyMetadata(DataTestUtility.AKVUrl, allowEnclaveComputations: fEnclaveEnabled);
            Assert.True(cmkSignature != null);

            // Expect failed verification for a toggle of enclaveEnabled bit
            Assert.False(fixture.AkvStoreProvider.VerifyColumnMasterKeyMetadata(DataTestUtility.AKVUrl, allowEnclaveComputations: !fEnclaveEnabled, signature: cmkSignature));

            // Prepare another cipherText buffer
            byte[] tamperedCmkSignature = new byte[cmkSignature.Length];
            Buffer.BlockCopy(cmkSignature, 0, tamperedCmkSignature, 0, tamperedCmkSignature.Length);

            // Corrupt one byte at a time 10 times
            RandomNumberGenerator rng = new RNGCryptoServiceProvider();
            byte[] randomIndexInCipherText = new byte[1];
            for (int i = 0; i < 10; i++)
            {
                Assert.True(fixture.AkvStoreProvider.VerifyColumnMasterKeyMetadata(DataTestUtility.AKVUrl, allowEnclaveComputations: fEnclaveEnabled, signature: tamperedCmkSignature), @"tamperedCmkSignature before tampering should be verified without any problems.");

                int startingByteIndex = 0;
                rng.GetBytes(randomIndexInCipherText);

                tamperedCmkSignature[startingByteIndex + randomIndexInCipherText[0]] = (byte)(cmkSignature[startingByteIndex + randomIndexInCipherText[0]] + 1);

                // Expect failed verification for invalid signature bytes
                Assert.False(fixture.AkvStoreProvider.VerifyColumnMasterKeyMetadata(DataTestUtility.AKVUrl, allowEnclaveComputations: fEnclaveEnabled, signature: tamperedCmkSignature));

                // Fix up the corrupted byte
                tamperedCmkSignature[startingByteIndex + randomIndexInCipherText[0]] = cmkSignature[startingByteIndex + randomIndexInCipherText[0]];
            }
        }

        [InlineData(new object[] { new string[] { null } })]
        [InlineData(new object[] { new string[] { "" } })]
        [InlineData(new object[] { new string[] { " " } })]
        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsAKVSetupAvailable))]
        public void InvalidTrustedEndpoints(string[] trustedEndpoints)
        {
            Exception ex = Assert.Throws<ArgumentException>(() =>
            {
                SqlColumnEncryptionAzureKeyVaultProvider azureKeyProvider = new SqlColumnEncryptionAzureKeyVaultProvider(
                    new SqlClientCustomTokenCredential(), trustedEndpoints);
            });
            Assert.Matches("One or more of the elements in trustedEndpoints are null or empty or consist of only whitespace.", ex.Message);
        }

        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsAKVSetupAvailable))]
        public void InvalidTrustedEndpoint(string trustedEndpoint)
        {
            Exception ex = Assert.Throws<ArgumentException>(() =>
            {
                SqlColumnEncryptionAzureKeyVaultProvider azureKeyProvider = new SqlColumnEncryptionAzureKeyVaultProvider(
                    new SqlClientCustomTokenCredential(), trustedEndpoint);
            });
            Assert.Matches("One or more of the elements in trustedEndpoints are null or empty or consist of only whitespace.", ex.Message);
        }
    }
}
