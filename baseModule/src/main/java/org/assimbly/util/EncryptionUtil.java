package org.assimbly.util;


import org.apache.commons.lang3.StringUtils;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.iv.RandomIvGenerator;

public class EncryptionUtil {

    private final StandardPBEStringEncryptor textEncryptor = new StandardPBEStringEncryptor();

    public EncryptionUtil(String password, String algorithm) {
        this.textEncryptor.setPassword(password);
        this.textEncryptor.setAlgorithm(algorithm);
        this.textEncryptor.setIvGenerator(new RandomIvGenerator());
    }

    public StandardPBEStringEncryptor getTextEncryptor() {
        return textEncryptor;
    }

    public String encrypt(String plainText) {
        //if value already encrypted do not encrypt and return
        if (plainText.startsWith("ENC(") && plainText.endsWith(")")) {
            return plainText;
        }
        return "ENC(" + this.textEncryptor.encrypt(plainText) + ")";
    }

    public String decrypt(String encryptedText){
        return this.textEncryptor.decrypt(encryptedText);
    }

}

